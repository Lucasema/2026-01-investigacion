# Pruebas del Transactional Outbox: la API y el relay.
import uuid

import pika
import psycopg
import pytest
from fastapi.testclient import TestClient

from app import api, config, relay, viajes
from tests.conftest import consultar, mensajes_en_cola

cliente_http = TestClient(api.app)
SOLICITUD = {"clienteId": "ana-gomez", "origen": "Av. Sarmiento 1500", "destino": "French 414"}
RABBITMQ_CAIDO = "amqp://guest:guest@localhost:1/%2F"  # nadie escucha en el puerto 1


def test_api_responde_202_y_guarda_viaje_y_evento_juntos():
    respuesta = cliente_http.post("/viajes", json=SOLICITUD)

    assert respuesta.status_code == 202
    viaje_id = respuesta.json()["viajeId"]
    assert respuesta.headers["Location"] == f"/viajes/{viaje_id}"
    assert consultar("SELECT estado FROM viajes WHERE id = %s", (viaje_id,)) == [("SOLICITADO",)]
    eventos = consultar("SELECT tipo, estado, payload->>'viajeId' FROM outbox")
    assert eventos == [("ViajeSolicitado", "PENDIENTE", viaje_id)]


def test_si_falla_el_insert_del_evento_no_queda_el_viaje():
    """Atomicidad: si el segundo INSERT falla, el primero también se deshace (ROLLBACK)."""
    evento_repetido = str(uuid.uuid4())
    viajes.solicitar_viaje_con_outbox("ana", "A", "B", evento_id=evento_repetido)

    with pytest.raises(psycopg.errors.UniqueViolation):
        # Mismo eventoId: el INSERT en outbox falla después de haber insertado el viaje
        viajes.solicitar_viaje_con_outbox("beto", "C", "D", evento_id=evento_repetido)

    assert consultar("SELECT cliente_id FROM viajes") == [("ana",)]  # el viaje de "beto" no quedó


def test_api_funciona_con_rabbitmq_caido():
    """La API con outbox no se conecta a RabbitMQ: responde 202 aunque el broker no esté."""
    config_original = config.RABBITMQ_URL
    config.RABBITMQ_URL = RABBITMQ_CAIDO
    try:
        respuesta = cliente_http.post("/viajes", json=SOLICITUD)
    finally:
        config.RABBITMQ_URL = config_original
    assert respuesta.status_code == 202
    assert consultar("SELECT estado FROM outbox") == [("PENDIENTE",)]


def test_relay_publica_los_pendientes_y_los_marca(canal):
    for _ in range(3):
        cliente_http.post("/viajes", json=SOLICITUD)

    publicados = relay.publicar_pendientes(canal)

    assert publicados == 3
    assert consultar("SELECT DISTINCT estado FROM outbox") == [("PUBLICADO",)]
    assert mensajes_en_cola(canal, config.COLA_ASIGNACION) == 3


def test_con_rabbitmq_caido_el_evento_queda_pendiente_y_se_publica_al_volver(canal):
    cliente_http.post("/viajes", json=SOLICITUD)

    # 1. Broker caído: no se puede ni conectar, el evento sigue PENDIENTE
    with pytest.raises(pika.exceptions.AMQPError):
        relay.mensajeria.conectar(RABBITMQ_CAIDO, confirmar_publicaciones=True)
    assert consultar("SELECT estado FROM outbox") == [("PENDIENTE",)]

    # 2. El broker vuelve: el relay publica el evento que había quedado pendiente
    assert relay.publicar_pendientes(canal) == 1
    assert consultar("SELECT estado FROM outbox") == [("PUBLICADO",)]
    assert mensajes_en_cola(canal, config.COLA_ASIGNACION) == 1


def test_si_el_broker_se_cae_al_publicar_el_evento_no_se_marca(canal):
    """El relay solo marca PUBLICADO si basic_publish terminó bien."""
    cliente_http.post("/viajes", json=SOLICITUD)

    class CanalQueFalla:
        def basic_publish(self, **kwargs):
            raise pika.exceptions.StreamLostError("conexión perdida")

    with pytest.raises(pika.exceptions.AMQPError):
        relay.publicar_pendientes(CanalQueFalla())
    assert consultar("SELECT estado FROM outbox") == [("PENDIENTE",)]


def test_sin_outbox_el_evento_se_pierde_si_el_broker_esta_caido(canal):
    """La versión ingenua guarda el viaje pero pierde el mensaje: nadie lo va a procesar."""
    config_original = config.RABBITMQ_URL
    config.RABBITMQ_URL = RABBITMQ_CAIDO
    try:
        respuesta = cliente_http.post("/viajes/sin-outbox", json=SOLICITUD)
    finally:
        config.RABBITMQ_URL = config_original

    assert respuesta.status_code == 500
    assert consultar("SELECT estado FROM viajes") == [("SOLICITADO",)]   # el viaje quedó guardado...
    assert consultar("SELECT count(*) FROM outbox") == [(0,)]            # ...sin evento pendiente
    assert mensajes_en_cola(canal, config.COLA_ASIGNACION) == 0          # ...y sin mensaje en la cola
