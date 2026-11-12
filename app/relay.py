# Relay del outbox: cada segundo publica en RabbitMQ los eventos PENDIENTE y los marca PUBLICADO.
# Si RabbitMQ está caído, los eventos quedan PENDIENTE y se reintentan en la vuelta siguiente.
import time

import pika
import psycopg

from app import config, mensajeria
from app.api import conectar_db

log = config.configurar_logs("relay")


def publicar_pendientes(canal) -> int:
    """Publica los eventos PENDIENTE en orden de creación y devuelve cuántos publicó."""
    publicados = 0
    with conectar_db() as conexion:
        pendientes = conexion.execute(
            "SELECT id, payload FROM outbox WHERE estado = 'PENDIENTE' ORDER BY creado_en"
        ).fetchall()
        for evento_id, evento in pendientes:
            # Con confirm_delivery, esta línea espera la confirmación de RabbitMQ.
            # Si no llega, lanza una excepción y el evento NO se marca.
            mensajeria.publicar(canal, str(evento_id), evento)
            # Si el relay se cae justo acá (publicado pero no marcado), al volver lo
            # publica otra vez: entrega "al menos una vez". Por eso el worker es idempotente.
            conexion.execute(
                "UPDATE outbox SET estado = 'PUBLICADO', publicado_en = now() WHERE id = %s",
                (evento_id,),
            )
            conexion.commit()
            publicados += 1
            log.info("evento %s publicado", evento_id)
    return publicados


def main() -> None:
    log.info("Relay iniciado: revisa el outbox cada %s s", config.INTERVALO_RELAY)
    canal = None
    while True:
        try:
            if canal is None or canal.is_closed:
                _, canal = mensajeria.conectar(confirmar_publicaciones=True)
                log.info("Conectado a RabbitMQ")
            publicar_pendientes(canal)
        except pika.exceptions.AMQPError:
            # Cualquier error de RabbitMQ (caído, conexión perdida, publicación no confirmada).
            # Ojo: el ConnectionError estándar de Python NO atrapa los errores de pika.
            log.warning("RabbitMQ no disponible: los eventos siguen PENDIENTE")
            canal = None  # en la próxima vuelta se reconecta
        except psycopg.OperationalError:
            log.warning("Base de datos no disponible, se reintenta")
        time.sleep(config.INTERVALO_RELAY)


if __name__ == "__main__":
    main()
