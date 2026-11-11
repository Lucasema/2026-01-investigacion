# Pruebas del worker: procesamiento, idempotencia y Dead Letter Queue.
import json
import threading

from fastapi.testclient import TestClient

from app import api, config, relay, worker
from tests.conftest import consultar, mensajes_en_cola

cliente_http = TestClient(api.app)
SOLICITUD = {"clienteId": "ana-gomez", "origen": "Av. Sarmiento 1500", "destino": "French 414"}


def consumir_uno(canal) -> None:
    """Toma un mensaje de la cola y lo procesa con el callback real del worker."""
    metodo, propiedades, cuerpo = canal.basic_get(config.COLA_ASIGNACION, auto_ack=False)
    assert metodo is not None, "no había mensajes en la cola"
    worker.procesar(canal, metodo, propiedades, cuerpo)


def test_flujo_completo_api_relay_worker(canal):
    viaje_id = cliente_http.post("/viajes", json=SOLICITUD).json()["viajeId"]
    relay.publicar_pendientes(canal)

    consumir_uno(canal)

    viaje = cliente_http.get(f"/viajes/{viaje_id}").json()
    assert viaje["estado"] == "CONDUCTOR_ASIGNADO"
    assert viaje["conductor"] in worker.CONDUCTORES
    assert mensajes_en_cola(canal, config.COLA_ASIGNACION) == 0  # se confirmó (ack) y se borró


def test_evento_duplicado_se_aplica_una_sola_vez(canal):
    """Simula que el relay publicó dos veces el mismo evento (se cayó antes de marcarlo)."""
    respuesta = cliente_http.post("/viajes", json=SOLICITUD).json()
    relay.publicar_pendientes(canal)
    consultar("UPDATE outbox SET estado = 'PENDIENTE' RETURNING id")  # "olvidó" que lo publicó
    relay.publicar_pendientes(canal)
    assert mensajes_en_cola(canal, config.COLA_ASIGNACION) == 2

    consumir_uno(canal)
    consumir_uno(canal)

    assert consultar("SELECT asignaciones FROM viajes WHERE id = %s", (respuesta["viajeId"],)) == [(1,)]
    assert consultar("SELECT count(*) FROM eventos_procesados") == [(1,)]
    assert mensajes_en_cola(canal, config.COLA_ASIGNACION) == 0  # los dos se confirmaron


def test_dos_workers_con_el_mismo_evento_a_la_vez():
    """Sin condición de carrera: con dos procesamientos simultáneos, solo uno aplica el cambio."""
    respuesta = cliente_http.post("/viajes", json=SOLICITUD).json()
    resultados = []
    barrera = threading.Barrier(2)

    def procesar():
        barrera.wait()
        resultados.append(worker.aplicar_asignacion(respuesta["eventoId"], respuesta["viajeId"], "Carlos R."))

    hilos = [threading.Thread(target=procesar) for _ in range(2)]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()

    assert sorted(resultados) == [False, True]
    assert consultar("SELECT asignaciones FROM viajes") == [(1,)]


def test_mensaje_invalido_va_a_la_dead_letter_queue(canal):
    canal.basic_publish(exchange="", routing_key=config.COLA_ASIGNACION, body=b"esto no es JSON")
    canal.basic_publish(exchange="", routing_key=config.COLA_ASIGNACION,
                        body=json.dumps({"viajeId": "sin-evento"}).encode())

    consumir_uno(canal)
    consumir_uno(canal)

    assert mensajes_en_cola(canal, config.COLA_ASIGNACION) == 0
    assert mensajes_en_cola(canal, config.COLA_DLQ) == 2


def test_mensaje_sin_ack_vuelve_a_la_cola(canal):
    """Si el worker se cae antes del ack, RabbitMQ devuelve el mensaje a la cola (Unacked -> Ready)."""
    cliente_http.post("/viajes", json=SOLICITUD)
    relay.publicar_pendientes(canal)

    from app import mensajeria
    conexion_worker, canal_worker = mensajeria.conectar()
    metodo, _, _ = canal_worker.basic_get(config.COLA_ASIGNACION, auto_ack=False)
    assert metodo is not None
    assert mensajes_en_cola(canal, config.COLA_ASIGNACION) == 0  # está Unacked
    conexion_worker.close()                                      # el worker "se cae" sin ack

    metodo, _, _ = canal.basic_get(config.COLA_ASIGNACION, auto_ack=False)
    assert metodo is not None and metodo.redelivered               # volvió y se marca como reentregado
    canal.basic_ack(metodo.delivery_tag)
