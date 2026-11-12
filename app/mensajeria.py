# Conexión con RabbitMQ, compartida por el relay, el worker y la versión sin outbox de la API.
import json

import pika

from app import config


def conectar(confirmar_publicaciones: bool = False):
    """Abre una conexión con RabbitMQ y devuelve (conexion, canal).

    confirmar_publicaciones=True activa los "publisher confirms": cada publicación espera
    a que RabbitMQ confirme que guardó el mensaje, y lanza una excepción si no lo hace.
    """
    parametros = pika.URLParameters(config.RABBITMQ_URL)
    parametros.connection_attempts = 1
    parametros.socket_timeout = 3
    try:
        conexion = pika.BlockingConnection(parametros)
    except OSError as error:
        # Con RabbitMQ apagado, Docker borra el nombre "rabbitmq" de la red y el error es de
        # Python ("ese nombre no existe"), no de pika. Se convierte para manejarlo igual que el resto.
        raise pika.exceptions.AMQPConnectionError(error) from error
    canal = conexion.channel()
    if confirmar_publicaciones:
        canal.confirm_delivery()
    # durable=True: la cola sobrevive a un reinicio de RabbitMQ
    canal.queue_declare(queue=config.COLA, durable=True)
    return conexion, canal


def publicar(canal, evento_id: str, evento: dict) -> None:
    canal.basic_publish(
        exchange="",
        routing_key=config.COLA,
        body=json.dumps(evento),
        # delivery_mode=2: el mensaje se guarda en disco (sobrevive a un reinicio)
        properties=pika.BasicProperties(delivery_mode=2, message_id=evento_id),
        mandatory=True,
    )
