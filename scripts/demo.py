# Comandos para manejar la demo. Se ejecutan dentro del contenedor de la API:
#
#   docker compose exec api python -m scripts.demo <comando>
#
# Comandos:
#   pedir [N] [--sin-outbox]   Solicita N viajes (por defecto 1)
#   estado <viajeId>           Muestra el estado de un viaje
#   resumen                    Viajes por estado, eventos del outbox por estado y mensajes en las colas
#   duplicar <eventoId>        Vuelve a poner un evento en PENDIENTE: el relay lo publica otra vez
#                              (simula que el relay se cayó entre publicar y marcar)
#   invalido                   Publica un mensaje mal formado: termina en la Dead Letter Queue
import json
import os
import sys
import time

import httpx
import pika

from app import config, mensajeria, viajes

API_URL = os.getenv("API_URL", "http://localhost:8000")
RABBITMQ_API = os.getenv("RABBITMQ_API", "http://rabbitmq:15672/api")
RABBITMQ_USUARIO = os.getenv("RABBITMQ_DEFAULT_USER", "demo")
RABBITMQ_CLAVE = os.getenv("RABBITMQ_DEFAULT_PASS", "demo")


def pedir(cantidad: int, sin_outbox: bool) -> None:
    ruta = "/viajes/sin-outbox" if sin_outbox else "/viajes"
    for i in range(cantidad):
        inicio = time.perf_counter()
        respuesta = httpx.post(f"{API_URL}{ruta}", json={
            "clienteId": f"cliente-{i + 1}", "origen": "Av. Sarmiento 1500", "destino": "French 414"})
        ms = (time.perf_counter() - inicio) * 1000
        print(f"{respuesta.status_code} en {ms:5.0f} ms  {respuesta.json()}")


def estado(viaje_id: str) -> None:
    respuesta = httpx.get(f"{API_URL}/viajes/{viaje_id}")
    print(json.dumps(respuesta.json(), indent=2, ensure_ascii=False, default=str))


def resumen() -> None:
    with viajes.conectar_db() as conexion:
        por_estado = conexion.execute(
            "SELECT estado, count(*) FROM viajes GROUP BY estado ORDER BY estado").fetchall()
        outbox = conexion.execute(
            "SELECT estado, count(*) FROM outbox GROUP BY estado ORDER BY estado").fetchall()
        dobles = conexion.execute("SELECT count(*) FROM viajes WHERE asignaciones > 1").fetchone()[0]
    print("Viajes:  ", dict(por_estado) or "-")
    print("Outbox:  ", dict(outbox) or "-")
    print("Viajes asignados más de una vez:", dobles)
    try:
        conexion, canal = mensajeria.conectar()
    except pika.exceptions.AMQPError:
        print("Colas: RabbitMQ no disponible")
        return
    for cola in (config.COLA_ASIGNACION, config.COLA_DLQ):
        # Ready: exacto, se lee de la cola. Unacked: de la consola web, con ~1 s de atraso.
        ready = canal.queue_declare(queue=cola, passive=True).method.message_count
        try:
            datos = httpx.get(f"{RABBITMQ_API}/queues/%2F/{cola}",
                              auth=(RABBITMQ_USUARIO, RABBITMQ_CLAVE), timeout=3).json()
            unacked = datos.get("messages_unacknowledged", 0)
        except (httpx.HTTPError, ValueError):
            unacked = "?"
        print(f"Cola {cola}: Ready={ready} Unacked={unacked}")
    conexion.close()


def duplicar(evento_id: str) -> None:
    with viajes.conectar_db() as conexion:
        filas = conexion.execute(
            "UPDATE outbox SET estado = 'PENDIENTE', publicado_en = NULL WHERE id = %s", (evento_id,)
        ).rowcount
    print("Evento vuelto a PENDIENTE: el relay lo va a publicar de nuevo." if filas else "No existe ese evento.")


def invalido() -> None:
    conexion, canal = mensajeria.conectar(confirmar_publicaciones=True)
    canal.basic_publish(exchange="", routing_key=config.COLA_ASIGNACION, body=b"esto no es JSON",
                        properties=mensajeria.propiedades_persistentes("mensaje-invalido"))
    conexion.close()
    print(f"Mensaje inválido publicado; el worker lo va a mandar a {config.COLA_DLQ}.")


def main(argumentos: list[str]) -> None:
    if not argumentos:
        print(__doc__ or "")
        sys.exit(1)
    comando, resto = argumentos[0], argumentos[1:]
    if comando == "pedir":
        numeros = [a for a in resto if a.isdigit()]
        pedir(int(numeros[0]) if numeros else 1, "--sin-outbox" in resto)
    elif comando == "estado" and resto:
        estado(resto[0])
    elif comando == "resumen":
        resumen()
    elif comando == "duplicar" and resto:
        duplicar(resto[0])
    elif comando == "invalido":
        invalido()
    else:
        print("Comando desconocido. Comandos: pedir, estado, resumen, duplicar, invalido")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
