# Conexión con RabbitMQ y declaración de colas, compartida por el relay, el worker y la API.
import pika

from app import config


def declarar_colas(canal) -> None:
    """Declara la cola principal (durable) y su Dead Letter Queue.

    Declarar es idempotente: si la cola ya existe con los mismos argumentos, no pasa nada.
    Por eso la llaman todos los procesos al conectarse, sin importar quién arranca primero.
    """
    canal.queue_declare(queue=config.COLA_DLQ, durable=True)
    canal.queue_declare(
        queue=config.COLA_ASIGNACION,
        durable=True,  # la cola sobrevive a un reinicio de RabbitMQ
        arguments={
            # Los mensajes rechazados sin reencolar (basic_nack con requeue=False) van a la DLQ
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": config.COLA_DLQ,
        },
    )


def conectar(url: str | None = None, confirmar_publicaciones: bool = False):
    """Abre una conexión y devuelve (conexion, canal) con las colas declaradas.

    Con confirmar_publicaciones=True activa los publisher confirms: cada basic_publish
    espera a que RabbitMQ confirme que guardó el mensaje y lanza una excepción si no.
    """
    parametros = pika.URLParameters(url or config.RABBITMQ_URL)
    parametros.connection_attempts = 1
    parametros.socket_timeout = 3
    parametros.blocked_connection_timeout = 5
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    if confirmar_publicaciones:
        canal.confirm_delivery()
    declarar_colas(canal)
    return conexion, canal


def propiedades_persistentes(message_id: str) -> pika.BasicProperties:
    # delivery_mode=2: RabbitMQ guarda el mensaje en disco (sobrevive a un reinicio)
    return pika.BasicProperties(
        delivery_mode=2,
        content_type="application/json",
        message_id=message_id,
    )
