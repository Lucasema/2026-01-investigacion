# Pruebas del Transactional Outbox: la API y el relay.
import pika
import psycopg
import pytest
from fastapi.testclient import TestClient

from app import api, config, relay
from tests.conftest import consultar, mensajes_en_cola

cliente = TestClient(api.app)
CALIFICACION = {"viajeId": "viaje-1", "conductorId": "carlos", "puntaje": 5}


def test_guarda_la_calificacion_y_el_evento_juntos():
    respuesta = cliente.post("/calificaciones", json=CALIFICACION)

    assert respuesta.status_code == 202
    assert consultar("SELECT conductor_id, puntaje FROM calificaciones") == [("carlos", 5)]
    assert consultar("SELECT tipo, estado FROM outbox") == [("ConductorCalificado", "PENDIENTE")]


def test_si_falla_el_evento_tampoco_queda_la_calificacion():
    """Atomicidad: si el segundo INSERT falla, el primero se deshace (ROLLBACK)."""
    datos = api.Calificacion(**CALIFICACION)
    _, evento_id = api.guardar_con_outbox(datos)

    with pytest.raises(psycopg.errors.UniqueViolation):
        api.guardar_con_outbox(datos, evento_id=evento_id)  # mismo eventoId: falla el INSERT en outbox

    assert consultar("SELECT count(*) FROM calificaciones") == [(1,)]  # la segunda no quedó


def test_el_relay_publica_y_marca_los_pendientes(canal):
    for _ in range(3):
        cliente.post("/calificaciones", json=CALIFICACION)

    assert relay.publicar_pendientes(canal) == 3

    assert consultar("SELECT DISTINCT estado FROM outbox") == [("PUBLICADO",)]
    assert mensajes_en_cola(canal) == 3


def test_con_rabbitmq_caido_el_evento_queda_pendiente_y_sale_al_volver(canal):
    cliente.post("/calificaciones", json=CALIFICACION)

    class CanalCaido:  # simula que RabbitMQ se cae al publicar
        def basic_publish(self, **kwargs):
            raise pika.exceptions.StreamLostError("conexión perdida")

    with pytest.raises(pika.exceptions.AMQPError):
        relay.publicar_pendientes(CanalCaido())
    assert consultar("SELECT estado FROM outbox") == [("PENDIENTE",)]  # no se perdió ni se marcó

    assert relay.publicar_pendientes(canal) == 1                      # RabbitMQ volvió
    assert consultar("SELECT estado FROM outbox") == [("PUBLICADO",)]


def test_sin_outbox_el_evento_se_pierde_con_rabbitmq_caido(canal, monkeypatch):
    # Como en Docker con RabbitMQ apagado: el nombre del servidor no existe
    monkeypatch.setattr(config, "RABBITMQ_URL", "amqp://demo:demo@rabbitmq-apagado.invalid:5672/%2F")

    respuesta = cliente.post("/calificaciones/sin-outbox", json=CALIFICACION)

    assert respuesta.status_code == 500
    assert consultar("SELECT count(*) FROM calificaciones") == [(1,)]  # la calificación quedó guardada...
    assert consultar("SELECT count(*) FROM outbox") == [(0,)]          # ...sin nada para reintentar
