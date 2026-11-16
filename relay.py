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

def obtener_conexion_broker():
    parametros = pika.ConnectionParameters(
        host="broker",
        port=5672,
        connection_attempts=1,
        socket_timeout=3
    )
    return pika.BlockingConnection(parametros)

def procesar_eventos_pendientes():
    conexion_bd = None
    conexion_broker = None
    eventos_pendientes = []
    try:
        conexion_bd = obtener_conexion_bd()
        cursor = conexion_bd.cursor()

        cursor.execute(
            """
            SELECT id, calificacion_id, payload
            FROM outbox_eventos
            WHERE estado = 'PENDIENTE'
            ORDER BY id ASC;
            """
        )
        eventos_pendientes = cursor.fetchall()

        if not eventos_pendientes:
            cursor.close()
            return

        conexion_broker = obtener_conexion_broker()
        canal = conexion_broker.channel()

        canal.queue_declare(queue="cola_calificaciones_m2", durable=True)
        canal.confirm_delivery()

        for fila in eventos_pendientes:
            evento_id = fila[0]
            calificacion_id = fila[1]
            payload_json = fila[2]

            print(f"[CALIFICACIÓN #{calificacion_id} | PASO 2 - RELAY <- BD] Evento #{evento_id} ('ConductorCalificado') leído con estado='PENDIENTE' desde 'outbox_eventos'.")
            print(f"[CALIFICACIÓN #{calificacion_id} | PASO 2 - RELAY -> BROKER] Publicando Evento #{evento_id} en la cola 'cola_calificaciones_m2' de RabbitMQ...")

            canal.basic_publish(
                exchange="",
                routing_key="cola_calificaciones_m2",
                body=payload_json,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json"
                )
            )

            cursor.execute(
                """
                UPDATE outbox_eventos
                SET estado = 'PUBLICADO'
                WHERE id = %s;
                """,
                (evento_id,)
            )
            conexion_bd.commit()
            print(f"[CALIFICACIÓN #{calificacion_id} | PASO 2 - RELAY -> BD] Confirmado por RabbitMQ. Tabla 'outbox_eventos' actualizada: Evento #{evento_id} pasó a estado='PUBLICADO'.")

        cursor.close()

    except (pika.exceptions.AMQPError, OSError) as error_broker:
        if conexion_bd is not None:
            conexion_bd.rollback()
        ids_calif = ", ".join([f"#{fila[1]}" for fila in eventos_pendientes])
        print(f"[CALIFICACIÓN {ids_calif} | PASO 2 - RELAY ALERTA] Broker RabbitMQ caído ({type(error_broker).__name__}). No se modifica la BD: sigue(n) en estado='PENDIENTE' en 'outbox_eventos'. Reintentando en 10s...")
        time.sleep(9)

    except Exception as error_general:
        if conexion_bd is not None:
            conexion_bd.rollback()
        print(f"[RELAY - ERROR] Fallo inesperado: {error_general}")
        time.sleep(9)

    finally:
        if conexion_broker is not None and conexion_broker.is_open:
            conexion_broker.close()
        if conexion_bd is not None:
            conexion_bd.close()

if __name__ == "__main__":
    print("[INICIO RELAY M2] Proceso Outbox Relay del Módulo Clientes activo (revisión rápida cada 1s; alertas cada 10s si cae el broker)...")
    while True:
        procesar_eventos_pendientes()
        time.sleep(1)