# Pruebas del worker: procesamiento, ack e idempotencia.
import threading

from fastapi.testclient import TestClient

from app import api, config, mensajeria, relay, worker
from tests.conftest import consultar, mensajes_en_cola

cliente = TestClient(api.app)
CALIFICACION = {"viajeId": "viaje-1", "conductorId": "carlos", "puntaje": 4}


def consumir_uno(canal) -> None:
    """Toma un mensaje de la cola y lo procesa con el código real del worker."""
    metodo, propiedades, cuerpo = canal.basic_get(config.COLA, auto_ack=False)
    assert metodo is not None, "la cola estaba vacía"
    worker.procesar(canal, metodo, propiedades, cuerpo)


def test_flujo_completo_suma_la_calificacion_al_promedio(canal):
    cliente.post("/calificaciones", json=CALIFICACION)
    relay.publicar_pendientes(canal)

    consumir_uno(canal)

    assert consultar("SELECT cantidad, suma FROM promedios_conductor") == [(1, 4)]
    assert mensajes_en_cola(canal) == 0  # se confirmó con ack y RabbitMQ lo borró


def test_un_evento_duplicado_se_cuenta_una_sola_vez(canal):
    """Simula que el relay publicó dos veces el mismo evento (se cayó antes de marcarlo)."""
    cliente.post("/calificaciones", json=CALIFICACION)
    relay.publicar_pendientes(canal)
    consultar("UPDATE outbox SET estado = 'PENDIENTE'")  # "se olvidó" de que lo publicó
    relay.publicar_pendientes(canal)
    assert mensajes_en_cola(canal) == 2

    consumir_uno(canal)
    consumir_uno(canal)

    assert consultar("SELECT cantidad, suma FROM promedios_conductor") == [(1, 4)]  # no (2, 8)


def test_dos_workers_con_el_mismo_evento_a_la_vez():
    """Sin condición de carrera: si dos procesan el mismo evento al mismo tiempo, cuenta uno solo."""
    _, evento_id = api.guardar_con_outbox(api.Calificacion(**CALIFICACION))
    resultados = []
    largada = threading.Barrier(2)

    def procesar():
        largada.wait()
        resultados.append(worker.sumar_al_promedio(evento_id, "carlos", 4))

    hilos = [threading.Thread(target=procesar) for _ in range(2)]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()

    assert sorted(resultados) == [False, True]
    assert consultar("SELECT cantidad FROM promedios_conductor") == [(1,)]


def test_un_mensaje_sin_ack_vuelve_a_la_cola(canal):
    """Si el worker se cae antes del ack, RabbitMQ devuelve el mensaje a la cola."""
    cliente.post("/calificaciones", json=CALIFICACION)
    relay.publicar_pendientes(canal)

    conexion_worker, canal_worker = mensajeria.conectar()
    metodo, _, _ = canal_worker.basic_get(config.COLA, auto_ack=False)  # lo toma...
    assert mensajes_en_cola(canal) == 0                                  # ...queda Unacked
    conexion_worker.close()                                              # ...y "se cae" sin ack

    metodo, _, _ = canal.basic_get(config.COLA, auto_ack=False)
    assert metodo is not None and metodo.redelivered  # volvió, marcado como reentregado
    canal.basic_ack(metodo.delivery_tag)
