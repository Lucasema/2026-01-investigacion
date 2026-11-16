import json
import psycopg2
import pika
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

app = FastAPI(title="TPI Módulo M2 (Clientes) - RF-2.5: Calificación del Conductor (Outbox + Colas)")

def obtener_conexion_bd():
    return psycopg2.connect(
        host="db",
        port=5432,
        dbname="bd_pedidos",
        user="usuario_tp",
        password="clave_tp"
    )

class SolicitudCalificarConductor(BaseModel):
    viaje_id: str = Field(default="viaje_101", examples=["viaje_101"])
    cliente_id: str = Field(default="cliente_matias", examples=["cliente_matias"])
    conductor_id: str = Field(default="conductor_carlos", examples=["conductor_carlos"])
    puntaje: int = Field(default=5, ge=1, le=5, examples=[5])
    comentario: str = Field(default="Excelente conducción y puntualidad", examples=["Excelente conducción y puntualidad"])

# =====================================================================
# 1. ENDPOINT PRINCIPAL DE M2 CON PATRÓN TRANSACTIONAL OUTBOX (SEGURO)
# =====================================================================
@app.post("/calificaciones", status_code=status.HTTP_202_ACCEPTED, summary="1. [M2 Clientes] Registrar calificación CON Patrón Outbox (Seguro)")
def registrar_calificacion(solicitud: SolicitudCalificarConductor):
    print(f"\n[PASO 1 - HTTP -> API M2] Solicitud POST /calificaciones recibida del '{solicitud.cliente_id}' (Puntaje: {solicitud.puntaje} estrellas para '{solicitud.conductor_id}').")
    conexion = None
    try:
        conexion = obtener_conexion_bd()
        cursor = conexion.cursor()

        # Insertar la calificación en la tabla 'calificaciones' de M2 con estado 'PENDIENTE_IMPACTO'
        cursor.execute(
            """
            INSERT INTO calificaciones (viaje_id, cliente_id, conductor_id, puntaje, comentario, estado)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (solicitud.viaje_id, solicitud.cliente_id, solicitud.conductor_id, solicitud.puntaje, solicitud.comentario, "PENDIENTE_IMPACTO")
        )
        nueva_calificacion_id = cursor.fetchone()[0]

        datos_evento = {
            "calificacion_id": nueva_calificacion_id,
            "viaje_id": solicitud.viaje_id,
            "cliente_id": solicitud.cliente_id,
            "conductor_id": solicitud.conductor_id,
            "puntaje": solicitud.puntaje,
            "comentario": solicitud.comentario
        }
        payload_json = json.dumps(datos_evento)

        # Insertar el evento en 'outbox_eventos' DENTRO DE LA MISMA TRANSACCIÓN LOCAL DE M2
        cursor.execute(
            """
            INSERT INTO outbox_eventos (calificacion_id, tipo_evento, payload, estado)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            (nueva_calificacion_id, "ConductorCalificado", payload_json, "PENDIENTE")
        )
        evento_id = cursor.fetchone()[0]

        # COMMIT único y atómico en PostgreSQL
        conexion.commit()
        cursor.close()

        print(f"[CALIFICACIÓN #{nueva_calificacion_id} | PASO 1 - API M2 -> BD] Transacción COMMIT exitosa: Tabla 'calificaciones' (estado='PENDIENTE_IMPACTO') y 'outbox_eventos' (Evento #{evento_id} estado='PENDIENTE').")

        return {
            "mensaje": "Calificación registrada en M2 (Clientes). El impacto en el perfil del conductor (M3) y auditoría (M8) se procesará asíncronamente.",
            "calificacion_id": nueva_calificacion_id,
            "evento_outbox_id": evento_id,
            "estado_actual": "PENDIENTE_IMPACTO"
        }

    except Exception as error:
        if conexion is not None:
            conexion.rollback()
        print(f"[API M2 - ERROR BD] Falló la transacción. Se ejecutó ROLLBACK de ambas tablas: {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error en la transacción de base de datos (se ejecutó ROLLBACK): {str(error)}"
        )
    finally:
        if conexion is not None:
            conexion.close()

# =====================================================================
# 2. ENDPOINT SIN OUTBOX (PARA DEMOSTRAR EL FALLO DE INCONSISTENCIA)
# =====================================================================
@app.post("/calificaciones/sin-outbox", status_code=status.HTTP_202_ACCEPTED, summary="2. [M2 Clientes] Registrar calificación SIN Outbox (Demuestra inconsistencia si cae el Broker)")
def registrar_calificacion_sin_outbox(solicitud: SolicitudCalificarConductor):
    print(f"[SIN OUTBOX - HTTP -> API M2] Solicitud POST /calificaciones/sin-outbox recibida del '{solicitud.cliente_id}'.")
    conexion = None
    nueva_calificacion_id = None
    try:
        conexion = obtener_conexion_bd()
        cursor = conexion.cursor()
        cursor.execute(
            """
            INSERT INTO calificaciones (viaje_id, cliente_id, conductor_id, puntaje, comentario, estado)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (solicitud.viaje_id, solicitud.cliente_id, solicitud.conductor_id, solicitud.puntaje, solicitud.comentario, "PENDIENTE_IMPACTO")
        )
        nueva_calificacion_id = cursor.fetchone()[0]
        conexion.commit()
        cursor.close()
        print(f"[CALIFICACIÓN #{nueva_calificacion_id} | SIN OUTBOX - API M2 -> BD] Tabla 'calificaciones' guardada con COMMIT (estado='PENDIENTE_IMPACTO').")
        print(f"[CALIFICACIÓN #{nueva_calificacion_id} | SIN OUTBOX - API M2 -> BROKER] Intentando publicar evento directo en RabbitMQ sin tabla Outbox...")

        parametros = pika.ConnectionParameters(host="broker", port=5672, connection_attempts=1, socket_timeout=3)
        conexion_broker = pika.BlockingConnection(parametros)
        canal = conexion_broker.channel()
        canal.queue_declare(queue="cola_calificaciones_m2", durable=True)

        payload_json = json.dumps({
            "calificacion_id": nueva_calificacion_id,
            "viaje_id": solicitud.viaje_id,
            "cliente_id": solicitud.cliente_id,
            "conductor_id": solicitud.conductor_id,
            "puntaje": solicitud.puntaje,
            "comentario": solicitud.comentario
        })
        canal.basic_publish(
            exchange="",
            routing_key="cola_calificaciones_m2",
            body=payload_json,
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
        )
        conexion_broker.close()

        return {
            "mensaje": "Calificación registrada y enviada directo a RabbitMQ (Sin Outbox).",
            "calificacion_id": nueva_calificacion_id,
            "estado_actual": "PENDIENTE_IMPACTO"
        }

    except (pika.exceptions.AMQPError, OSError) as error_broker:
        print(f"[CALIFICACIÓN #{nueva_calificacion_id} | SIN OUTBOX - INCONSISTENCIA GRAVE] RabbitMQ caído ({type(error_broker).__name__}). La Calificación #{nueva_calificacion_id} quedó guardada en M2, pero el evento hacia M3 SE PERDIÓ para siempre.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "¡INCONSISTENCIA ENTRE MÓDULOS DETECTADA! (Problema Dual-Write)",
                "explicacion": f"La Calificación ID={nueva_calificacion_id} ya hizo COMMIT en la base de datos de M2 (Clientes), pero RabbitMQ estaba caído ({type(error_broker).__name__}). Sin tabla Outbox, el evento se perdió y M3 (Conductores) jamás actualizará el promedio del conductor.",
                "calificacion_id_huerfana": nueva_calificacion_id
            }
        )
    finally:
        if conexion is not None:
            conexion.close()

# =====================================================================
# 3. ENDPOINT PARA SIMULAR DUPLICADO Y DEMOSTRAR IDEMPOTENCIA (RF-2.5 y RNF-09)
# =====================================================================
@app.post("/calificaciones/{calificacion_id}/forzar-duplicado", summary="3. Reenviar evento de una calificación (Demuestra Idempotencia RNF-09 / RF-2.5)")
def forzar_duplicado_outbox(calificacion_id: int):
    conexion = obtener_conexion_bd()
    cursor = conexion.cursor()

    cursor.execute(
        """
        UPDATE outbox_eventos
        SET estado = 'PENDIENTE'
        WHERE calificacion_id = %s
        RETURNING id;
        """,
        (calificacion_id,)
    )
    fila = cursor.fetchone()
    conexion.commit()
    cursor.close()
    conexion.close()

    if fila is None:
        raise HTTPException(status_code=404, detail=f"No existe un evento Outbox para la calificación {calificacion_id}.")

    print(f"[CALIFICACIÓN #{calificacion_id} | SIMULACIÓN DUPLICADO - API M2 -> BD] Evento #{fila[0]} en 'outbox_eventos' devuelto a estado='PENDIENTE' para forzar segundo envío.")
    return {
        "mensaje": f"El evento ID={fila[0]} de la Calificación ID={calificacion_id} volvió a estado PENDIENTE.",
        "efecto_esperado": "En 1 segundo, el Relay volverá a publicar el evento en RabbitMQ y el Worker demostrará su idempotencia (RF-2.5 y RNF-09) descartando el duplicado para no sumar dos veces la misma calificación al conductor."
    }

# =====================================================================
# 4. ENDPOINT DE RESUMEN GENERAL DE M2 (BD + RABBITMQ EN VIVO)
# =====================================================================
@app.get("/resumen", summary="4. Ver resumen general de Calificaciones (M2), Outbox y Cola de RabbitMQ")
def resumen_sistema():
    conexion = obtener_conexion_bd()
    cursor = conexion.cursor()

    cursor.execute("SELECT estado, COUNT(*) FROM calificaciones GROUP BY estado;")
    estados_calificaciones = dict(cursor.fetchall())

    cursor.execute("SELECT estado, COUNT(*) FROM outbox_eventos GROUP BY estado;")
    estados_outbox = dict(cursor.fetchall())

    cursor.execute(
        """
        SELECT id FROM calificaciones
        WHERE id NOT IN (SELECT calificacion_id FROM outbox_eventos)
        AND estado = 'PENDIENTE_IMPACTO'
        ORDER BY id ASC;
        """
    )
    calificaciones_huerfanas = [fila[0] for fila in cursor.fetchall()]

    cursor.close()
    conexion.close()

    try:
        parametros = pika.ConnectionParameters(host="broker", port=5672, connection_attempts=1, socket_timeout=2)
        conexion_broker = pika.BlockingConnection(parametros)
        canal = conexion_broker.channel()
        estado_cola = canal.queue_declare(queue="cola_calificaciones_m2", durable=True, passive=True)
        mensajes_ready_en_cola = estado_cola.method.message_count
        conexion_broker.close()
        info_rabbit = f"{mensajes_ready_en_cola} mensaje(s) esperando en estado Ready"
    except (pika.exceptions.AMQPError, OSError):
        info_rabbit = "Broker RabbitMQ APAGADO / No disponible"

    return {
        "tabla_calificaciones_m2": {
            "PENDIENTE_IMPACTO": estados_calificaciones.get("PENDIENTE_IMPACTO", 0),
            "IMPACTADA_EN_CONDUCTOR": estados_calificaciones.get("IMPACTADA_EN_CONDUCTOR", 0)
        },
        "tabla_outbox_eventos_m2": {
            "PENDIENTE": estados_outbox.get("PENDIENTE", 0),
            "PUBLICADO": estados_outbox.get("PUBLICADO", 0)
        },
        "cola_rabbitmq_en_vivo": info_rabbit,
        "calificaciones_inconsistentes_sin_outbox_id": calificaciones_huerfanas
    }

# =====================================================================
# 5. ENDPOINT DE CONSULTA INDIVIDUAL POR ID DE CALIFICACIÓN
# =====================================================================
@app.get("/calificaciones/{calificacion_id}", summary="5. Consultar el estado de una calificación específica y su evento Outbox")
def consultar_estado_calificacion(calificacion_id: int):
    conexion = obtener_conexion_bd()
    cursor = conexion.cursor()

    cursor.execute(
        "SELECT id, viaje_id, cliente_id, conductor_id, puntaje, comentario, estado, fecha_creacion FROM calificaciones WHERE id = %s;",
        (calificacion_id,)
    )
    fila_calif = cursor.fetchone()

    if fila_calif is None:
        cursor.close()
        conexion.close()
        raise HTTPException(status_code=404, detail="Calificación no encontrada")

    cursor.execute(
        "SELECT id, tipo_evento, estado, payload FROM outbox_eventos WHERE calificacion_id = %s;",
        (calificacion_id,)
    )
    fila_outbox = cursor.fetchone()

    cursor.close()
    conexion.close()

    return {
        "tabla_calificaciones_m2": {
            "id": fila_calif[0],
            "viaje_id": fila_calif[1],
            "cliente_id": fila_calif[2],
            "conductor_id": fila_calif[3],
            "puntaje": fila_calif[4],
            "comentario": fila_calif[5],
            "estado": fila_calif[6],
            "fecha_creacion": str(fila_calif[7])
        },
        "tabla_outbox_eventos_m2": {
            "id": fila_outbox[0],
            "tipo_evento": fila_outbox[1],
            "estado": fila_outbox[2],
            "payload": json.loads(fila_outbox[3])
        } if fila_outbox else "INCONSISTENTE: Esta calificación se guardó SIN patrón Outbox y su evento hacia M3 se perdió."
    }