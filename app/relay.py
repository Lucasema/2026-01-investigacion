# Relay del outbox (polling publisher).
#
# Cada INTERVALO_RELAY segundos lee los eventos PENDIENTE de la tabla outbox, los publica
# en RabbitMQ con confirmación del broker y recién entonces los marca PUBLICADO.
# Si RabbitMQ está caído, los eventos quedan PENDIENTE y se reintentan en la vuelta siguiente.
import json
import time

import pika
import psycopg

from app import config, mensajeria, viajes

log = config.configurar_logs("relay")


def publicar_pendientes(canal, limite: int = 100) -> int:
    """Publica los eventos pendientes en orden de creación. Devuelve cuántos publicó.

    Si RabbitMQ falla a mitad de camino, lanza la excepción: los eventos ya publicados
    quedaron marcados y el resto sigue PENDIENTE.
    """
    publicados = 0
    with viajes.conectar_db() as conexion:
        pendientes = conexion.execute(
            "SELECT id, tipo, payload FROM outbox WHERE estado = 'PENDIENTE' "
            "ORDER BY creado_en LIMIT %s",
            (limite,),
        ).fetchall()
        for evento_id, tipo, payload in pendientes:
            evento_id = str(evento_id)
            # Con confirm_delivery activado, basic_publish espera la confirmación del broker
            # y lanza una excepción de pika si no la recibe.
            canal.basic_publish(
                exchange="",
                routing_key=config.COLA_ASIGNACION,
                body=json.dumps({"eventoId": evento_id, "tipo": tipo, **payload}),
                properties=mensajeria.propiedades_persistentes(evento_id),
                mandatory=True,
            )
            # Si el proceso se cae justo acá (publicado pero no marcado), al reiniciar
            # lo publica otra vez: entrega "al menos una vez". El worker es idempotente.
            conexion.execute(
                "UPDATE outbox SET estado = 'PUBLICADO', publicado_en = now() WHERE id = %s",
                (evento_id,),
            )
            conexion.commit()
            publicados += 1
            log.info("evento=%s viaje=%s publicado", evento_id, payload.get("viajeId"))
    return publicados


def main() -> None:
    log.info("Relay iniciado: revisa el outbox cada %s s", config.INTERVALO_RELAY)
    conexion_rabbit = canal = None
    while True:
        try:
            if canal is None or canal.is_closed:
                conexion_rabbit, canal = mensajeria.conectar(confirmar_publicaciones=True)
                log.info("Conectado a RabbitMQ")
            publicar_pendientes(canal)
        except pika.exceptions.AMQPError as error:
            # Incluye conexión rechazada, conexión perdida y publicación no confirmada.
            # (El ConnectionError estándar de Python NO atrapa los errores de pika.)
            log.warning("RabbitMQ no disponible, los eventos siguen PENDIENTE: %s", error.__class__.__name__)
            if conexion_rabbit is not None and conexion_rabbit.is_open:
                try:
                    conexion_rabbit.close()
                except pika.exceptions.AMQPError:
                    pass
            conexion_rabbit = canal = None
        except psycopg.OperationalError as error:
            log.warning("Base de datos no disponible, se reintenta: %s", error)
        time.sleep(config.INTERVALO_RELAY)


if __name__ == "__main__":
    main()
