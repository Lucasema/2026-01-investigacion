# Comandos para la demo. Se ejecutan dentro del contenedor de la API:
#
#   docker compose exec api python -m scripts.demo <comando>
#
#   calificar [N] [--sin-outbox]   El cliente califica N veces al conductor "carlos" (por defecto 1)
#   resumen                        Muestra calificaciones, outbox, promedio y mensajes en la cola
#   duplicar                       Hace que el relay publique otra vez el último evento
import json
import sys
import time
import urllib.error
import urllib.request

import pika

from app import config, mensajeria
from app.api import conectar_db

API_URL = "http://localhost:8000"
PUNTAJES = [5, 4, 5, 3, 4]


def calificar(cantidad: int, sin_outbox: bool) -> None:
    ruta = "/calificaciones/sin-outbox" if sin_outbox else "/calificaciones"
    for i in range(cantidad):
        cuerpo = {"viajeId": f"viaje-{int(time.time() * 1000)}", "conductorId": "carlos",
                  "puntaje": PUNTAJES[i % len(PUNTAJES)]}
        pedido = urllib.request.Request(API_URL + ruta, data=json.dumps(cuerpo).encode(),
                                        headers={"Content-Type": "application/json"})
        inicio = time.perf_counter()
        try:
            with urllib.request.urlopen(pedido) as respuesta:
                codigo, texto = respuesta.status, respuesta.read().decode()
        except urllib.error.HTTPError as error:
            codigo, texto = error.code, error.read().decode()
        print(f"{codigo} en {(time.perf_counter() - inicio) * 1000:.0f} ms  {texto}")


def resumen() -> None:
    with conectar_db() as conexion:
        guardadas = conexion.execute(
            "SELECT count(*) FROM calificaciones WHERE conductor_id = 'carlos'").fetchone()[0]
        outbox = dict(conexion.execute("SELECT estado, count(*) FROM outbox GROUP BY estado").fetchall())
        fila = conexion.execute(
            "SELECT cantidad, suma FROM promedios_conductor WHERE conductor_id = 'carlos'").fetchone()
    contadas, suma = fila or (0, 0)
    promedio = f"{suma / contadas:.2f}" if contadas else "-"
    print(f"M2 · calificaciones guardadas de carlos: {guardadas}")
    print(f"M2 · outbox: {outbox or 'vacío'}")
    print(f"M3 · calificaciones contadas en el promedio: {contadas}  (promedio {promedio})")
    try:
        conexion_rabbit, canal = mensajeria.conectar()
        en_cola = canal.queue_declare(queue=config.COLA, passive=True).method.message_count
        conexion_rabbit.close()
        print(f"Cola '{config.COLA}': {en_cola} mensajes esperando (Ready)")
    except pika.exceptions.AMQPError:
        print("Cola: RabbitMQ no disponible")


def duplicar() -> None:
    """Simula que el relay se cayó después de publicar y antes de marcar el evento."""
    with conectar_db() as conexion:
        fila = conexion.execute(
            "UPDATE outbox SET estado = 'PENDIENTE' WHERE id = ("
            "SELECT id FROM outbox WHERE estado = 'PUBLICADO' ORDER BY publicado_en DESC LIMIT 1"
            ") RETURNING id").fetchone()
    print(f"El evento {fila[0]} volvió a PENDIENTE: el relay lo va a publicar otra vez."
          if fila else "No hay eventos publicados para duplicar.")


if __name__ == "__main__":
    argumentos = sys.argv[1:]
    comando = argumentos[0] if argumentos else ""
    if comando == "calificar":
        numeros = [a for a in argumentos if a.isdigit()]
        calificar(int(numeros[0]) if numeros else 1, "--sin-outbox" in argumentos)
    elif comando == "resumen":
        resumen()
    elif comando == "duplicar":
        duplicar()
    else:
        print("Comandos: calificar [N] [--sin-outbox] | resumen | duplicar")
