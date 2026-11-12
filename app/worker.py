# Worker (consumidor): hace de M3 (Conductores). Toma los eventos ConductorCalificado de la cola
# y suma cada calificación al promedio del conductor.
#
# Es IDEMPOTENTE: si el mismo evento llega dos veces, la calificación se cuenta una sola vez.
import json
import time

import pika
import psycopg

from app import config, mensajeria
from app.api import conectar_db

log = config.configurar_logs("worker")


def sumar_al_promedio(evento_id: str, conductor_id: str, puntaje: int) -> bool:
    """Suma la calificación al promedio. Devuelve False si el evento ya estaba procesado.

    Las dos escrituras van en UNA transacción:
    1. Registra el evento. ON CONFLICT DO NOTHING: si ya existía, no inserta nada (rowcount 0).
    2. Solo si es nuevo, suma el puntaje (crea la fila del conductor si es su primera calificación).
    """
    with conectar_db() as conexion:
        nuevo = conexion.execute(
            "INSERT INTO eventos_procesados (evento_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (evento_id,),
        ).rowcount
        if nuevo == 0:
            return False
        conexion.execute(
            "INSERT INTO promedios_conductor (conductor_id, cantidad, suma) VALUES (%s, 1, %s) "
            "ON CONFLICT (conductor_id) DO UPDATE SET "
            "cantidad = promedios_conductor.cantidad + 1, suma = promedios_conductor.suma + %s",
            (conductor_id, puntaje, puntaje),
        )
    return True


def procesar(canal, metodo, propiedades, cuerpo) -> None:
    """Se ejecuta por cada mensaje que entrega RabbitMQ."""
    try:
        evento = json.loads(cuerpo)
        evento_id, conductor_id, puntaje = evento["eventoId"], evento["conductorId"], int(evento["puntaje"])
    except (ValueError, KeyError):
        log.error("Mensaje inválido: se descarta")
        canal.basic_reject(delivery_tag=metodo.delivery_tag, requeue=False)
        return

    reentregado = " (reentregado)" if metodo.redelivered else ""
    log.info("evento %s recibido%s: conductor %s, puntaje %s", evento_id, reentregado, conductor_id, puntaje)
    time.sleep(config.TIEMPO_PROCESAMIENTO)  # simula un trabajo lento

    try:
        sumado = sumar_al_promedio(evento_id, conductor_id, puntaje)
    except psycopg.Error:
        log.warning("evento %s: error de base de datos, vuelve a la cola", evento_id)
        canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=True)
        return

    if sumado:
        log.info("evento %s: calificación sumada al promedio de %s", evento_id, conductor_id)
    else:
        log.info("evento %s DUPLICADO: ya estaba contado, no se suma otra vez", evento_id)
    # El ack va al final: si el worker se cae antes de esta línea, RabbitMQ devuelve
    # el mensaje a la cola y lo entrega de nuevo.
    canal.basic_ack(delivery_tag=metodo.delivery_tag)


def main() -> None:
    log.info("Worker iniciado: %s s por mensaje", config.TIEMPO_PROCESAMIENTO)
    while True:
        try:
            _, canal = mensajeria.conectar()
            canal.basic_qos(prefetch_count=1)  # de a un mensaje por vez
            canal.basic_consume(queue=config.COLA, on_message_callback=procesar, auto_ack=False)
            log.info("Conectado a RabbitMQ, esperando mensajes")
            canal.start_consuming()
        except pika.exceptions.AMQPError:
            log.warning("RabbitMQ no disponible, se reintenta en 2 s")
            time.sleep(2)


if __name__ == "__main__":
    main()
