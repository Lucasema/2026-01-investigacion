# Worker de asignación (consumidor).
#
# Toma mensajes ViajeSolicitado de la cola, simula la búsqueda de un conductor (operación lenta)
# y asigna el conductor al viaje. Es IDEMPOTENTE: si el mismo evento llega dos veces,
# el efecto se aplica una sola vez.
import json
import random
import time

import pika
import psycopg

from app import config, mensajeria, viajes

log = config.configurar_logs("worker")

CONDUCTORES = ["Carlos R. (AE123CD)", "Laura M. (AF456GH)", "Diego P. (AB789JK)", "Sofía T. (AD321LM)"]


class MensajeInvalido(Exception):
    """El mensaje no tiene el formato esperado: reintentarlo no sirve, va a la DLQ."""


def leer_mensaje(cuerpo: bytes) -> dict:
    try:
        mensaje = json.loads(cuerpo)
        if not isinstance(mensaje, dict) or not mensaje.get("eventoId") or not mensaje.get("viajeId"):
            raise ValueError("faltan eventoId o viajeId")
        return mensaje
    except ValueError as error:  # json.JSONDecodeError también es ValueError
        raise MensajeInvalido(str(error)) from error


def buscar_conductor(viaje_id: str) -> str:
    """Simula la operación lenta (buscar conductores cercanos, enviar ofertas, esperar respuesta)."""
    time.sleep(config.TIEMPO_PROCESAMIENTO)
    return random.choice(CONDUCTORES)


def aplicar_asignacion(evento_id: str, viaje_id: str, conductor: str) -> bool:
    """Registra el evento y asigna el conductor en UNA transacción.

    Devuelve True si aplicó el cambio y False si el evento ya estaba procesado.
    ON CONFLICT DO NOTHING inserta solo la primera vez. Si dos workers procesan el mismo
    evento a la vez, PostgreSQL hace esperar al segundo INSERT hasta que el primero confirma,
    y entonces no inserta nada: solo uno aplica el cambio.
    """
    with viajes.conectar_db() as conexion:
        nuevo = conexion.execute(
            "INSERT INTO eventos_procesados (evento_id) VALUES (%s) ON CONFLICT (evento_id) DO NOTHING",
            (evento_id,),
        ).rowcount
        if nuevo == 0:
            return False
        conexion.execute(
            "UPDATE viajes SET estado = 'CONDUCTOR_ASIGNADO', conductor = %s, "
            "asignaciones = asignaciones + 1, actualizado_en = now() WHERE id = %s",
            (conductor, viaje_id),
        )
    return True


def procesar(canal, metodo, propiedades, cuerpo) -> None:
    """Callback de pika: se ejecuta por cada mensaje recibido."""
    try:
        mensaje = leer_mensaje(cuerpo)
    except MensajeInvalido as error:
        log.error("Mensaje inválido, se envía a la DLQ: %s", error)
        canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
        return

    evento_id, viaje_id = mensaje["eventoId"], mensaje["viajeId"]
    reentrega = " (reentregado)" if metodo.redelivered else ""
    log.info("evento=%s viaje=%s recibido%s; buscando conductor...", evento_id, viaje_id, reentrega)

    conductor = buscar_conductor(viaje_id)  # lento, todavía sin efectos guardados
    try:
        aplicado = aplicar_asignacion(evento_id, viaje_id, conductor)
    except psycopg.Error as error:
        # Falla de la base: se devuelve el mensaje a la cola para reintentar más tarde
        log.warning("evento=%s error de base, se reencola: %s", evento_id, error)
        time.sleep(1)
        canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=True)
        return

    if aplicado:
        log.info("evento=%s viaje=%s CONDUCTOR_ASIGNADO: %s", evento_id, viaje_id, conductor)
    else:
        log.info("evento=%s DUPLICADO: ya estaba procesado, se confirma sin repetir el efecto", evento_id)
    # Recién ahora RabbitMQ puede borrar el mensaje. Si el worker se cae antes de esta
    # línea, el mensaje vuelve a la cola (Unacked -> Ready) y se reentrega.
    canal.basic_ack(delivery_tag=metodo.delivery_tag)


def main() -> None:
    log.info("Worker iniciado (cola %s, %s s por tarea)", config.COLA_ASIGNACION, config.TIEMPO_PROCESAMIENTO)
    while True:
        try:
            conexion, canal = mensajeria.conectar()
            canal.basic_qos(prefetch_count=1)  # un mensaje a la vez: consumo de recursos constante
            canal.basic_consume(queue=config.COLA_ASIGNACION, on_message_callback=procesar, auto_ack=False)
            log.info("Conectado a RabbitMQ, esperando mensajes")
            canal.start_consuming()
        except pika.exceptions.AMQPError as error:
            log.warning("RabbitMQ no disponible, se reintenta en 2 s: %s", error.__class__.__name__)
            time.sleep(2)


if __name__ == "__main__":
    main()
