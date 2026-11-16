import json
import time
import psycopg2
import pika

def obtener_conexion_bd():
    return psycopg2.connect(
        host="db",
        port=5432,
        dbname="bd_pedidos",
        user="usuario_tp",
        password="clave_tp"
    )

def procesar_calificacion(canal, method, properties, body):
    # Micropausa de 0.2s para que Docker muestre primero todos los logs del PASO 2 (Relay)
    time.sleep(0.2)

    datos_evento = json.loads(body.decode("utf-8"))
    calificacion_id = datos_evento["calificacion_id"]
    conductor_id = datos_evento["conductor_id"]
    puntaje = datos_evento["puntaje"]

    aviso_reentrega = " [REENTREGADO por RabbitMQ tras caída previa sin ACK]" if method.redelivered else ""
    print(f"[CALIFICACIÓN #{calificacion_id} | PASO 3 - WORKER <- BROKER] Evento 'ConductorCalificado' recibido de RabbitMQ (Conductor: {conductor_id}, Puntaje: {puntaje} estrellas){aviso_reentrega}.")

    conexion_bd = None
    try:
        conexion_bd = obtener_conexion_bd()
        cursor = conexion_bd.cursor()

        # Paso A: Control de Idempotencia (RF-2.5 y RNF-09: evitar impactar dos veces la misma calificación)
        cursor.execute("SELECT estado FROM calificaciones WHERE id = %s;", (calificacion_id,))
        fila = cursor.fetchone()

        if fila is not None and fila[0] == "IMPACTADA_EN_CONDUCTOR":
            print(f"[CALIFICACIÓN #{calificacion_id} | PASO 3 - WORKER IDEMPOTENCIA] Consulta en BD: la Calificación #{calificacion_id} ya tiene estado='IMPACTADA_EN_CONDUCTOR'. Se descarta el duplicado (RF-2.5 / RNF-09) y se envía ACK a RabbitMQ.")
            cursor.close()
            canal.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Paso B: Simulación de operación lenta (40s: recálculo de reputación/habilitación en M3 y auditoría en M8)
        print(f"[CALIFICACIÓN #{calificacion_id} | PASO 3 - WORKER PROCESANDO] Recalculando reputación del conductor en M3 y auditando reseña en M8 (demora 40s; mensaje en estado Unacked en RabbitMQ)...")
        time.sleep(40)

        # Paso C: Actualizar la tabla 'calificaciones' a 'IMPACTADA_EN_CONDUCTOR'
        cursor.execute(
            """
            UPDATE calificaciones
            SET estado = 'IMPACTADA_EN_CONDUCTOR'
            WHERE id = %s;
            """,
            (calificacion_id,)
        )
        conexion_bd.commit()
        cursor.close()

        print(f"[CALIFICACIÓN #{calificacion_id} | PASO 4 - WORKER -> BD] Transacción COMMIT en BD: Tabla 'calificaciones' actualizada, estado pasó de 'PENDIENTE_IMPACTO' a 'IMPACTADA_EN_CONDUCTOR'.")

        # Paso D: Enviar acuse de recibo (ACK) al Broker RabbitMQ
        canal.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[CALIFICACIÓN #{calificacion_id} | PASO 4 - WORKER -> BROKER] Señal ACK enviada a RabbitMQ. Mensaje eliminado de la cola. Ciclo completado.\n")

    except Exception as error:
        if conexion_bd is not None:
            conexion_bd.rollback()
        print(f"[CALIFICACIÓN #{calificacion_id} | WORKER - ERROR BD] Falló la actualización en BD ({error}). Se ejecuta ROLLBACK y se envía NACK a RabbitMQ para reencolar.")
        canal.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    finally:
        if conexion_bd is not None:
            conexion_bd.close()

def iniciar_worker():
    while True:
        try:
            print("[INICIO WORKER] Conectando al Broker RabbitMQ en el puerto 5672...")
            parametros = pika.ConnectionParameters(host="broker", port=5672, heartbeat=300)
            conexion_broker = pika.BlockingConnection(parametros)
            canal = conexion_broker.channel()

            canal.queue_declare(queue="cola_calificaciones_m2", durable=True)
            canal.basic_qos(prefetch_count=1)

            canal.basic_consume(
                queue="cola_calificaciones_m2",
                on_message_callback=procesar_calificacion,
                auto_ack=False
            )

            print("[INICIO WORKER] Escuchando 'cola_calificaciones_m2' (prefetch_count=1, auto_ack=False). Listo para trabajar...")
            canal.start_consuming()

        except (pika.exceptions.AMQPError, OSError) as error_conexion:
            print(f"[WORKER - ALERTA] Sin conexión con RabbitMQ ({type(error_conexion).__name__}). Reintentando en 10 segundos...")
            time.sleep(10)

if __name__ == "__main__":
    iniciar_worker()