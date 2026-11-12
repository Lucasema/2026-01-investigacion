# API de M2 (Clientes): el cliente califica al conductor después de un viaje.
#
# POST /calificaciones             CON outbox: guarda la calificación y el evento juntos.
# POST /calificaciones/sin-outbox  SIN outbox: guarda y publica directo (para comparar).
#
# El promedio del conductor lo calcula después el worker, que hace de M3 (Conductores).
import uuid

import pika
import psycopg
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from app import config, mensajeria

log = config.configurar_logs("api")

app = FastAPI(title="Demo: colas de mensajes + Transactional Outbox")


class Calificacion(BaseModel):
    viajeId: str = Field(min_length=1, examples=["viaje-123"])
    conductorId: str = Field(min_length=1, examples=["carlos"])
    puntaje: int = Field(ge=1, le=5, examples=[5])


def conectar_db():
    return psycopg.connect(config.DATABASE_URL, connect_timeout=3)


def guardar_con_outbox(datos: Calificacion, evento_id: str | None = None) -> tuple[str, str]:
    """Transactional Outbox: la calificación y el evento se guardan en UNA transacción.

    psycopg abre la transacción con el primer comando. Al salir del bloque "with" hace
    COMMIT; si algo falla, ROLLBACK de las dos filas. Esta función no toca RabbitMQ.
    """
    calificacion_id = str(uuid.uuid4())
    evento_id = evento_id or str(uuid.uuid4())
    evento = {"eventoId": evento_id, "conductorId": datos.conductorId, "puntaje": datos.puntaje}

    with conectar_db() as conexion:
        conexion.execute(
            "INSERT INTO calificaciones (id, viaje_id, conductor_id, puntaje) VALUES (%s, %s, %s, %s)",
            (calificacion_id, datos.viajeId, datos.conductorId, datos.puntaje),
        )
        conexion.execute(
            "INSERT INTO outbox (id, tipo, payload) VALUES (%s, 'ConductorCalificado', %s)",
            (evento_id, Jsonb(evento)),
        )
    return calificacion_id, evento_id


@app.post("/calificaciones", status_code=202)
def calificar(datos: Calificacion):
    calificacion_id, evento_id = guardar_con_outbox(datos)
    log.info("calificación %s guardada junto con el evento %s (PENDIENTE)", calificacion_id, evento_id)
    # 202 Accepted: la calificación quedó registrada; el promedio se actualiza después
    return JSONResponse({"calificacionId": calificacion_id, "eventoId": evento_id}, status_code=202)


@app.post("/calificaciones/sin-outbox", status_code=202)
def calificar_sin_outbox(datos: Calificacion):
    """Versión SIN outbox (escritura dual), solo para mostrar el problema."""
    calificacion_id = str(uuid.uuid4())
    # 1. Guarda la calificación: al salir del "with" ya se hizo COMMIT
    with conectar_db() as conexion:
        conexion.execute(
            "INSERT INTO calificaciones (id, viaje_id, conductor_id, puntaje) VALUES (%s, %s, %s, %s)",
            (calificacion_id, datos.viajeId, datos.conductorId, datos.puntaje),
        )
    # 2. Publica directo en RabbitMQ. Si falla, el evento se pierde: no quedó anotado en ningún lado
    evento = {"eventoId": calificacion_id, "conductorId": datos.conductorId, "puntaje": datos.puntaje}
    try:
        conexion_rabbit, canal = mensajeria.conectar(confirmar_publicaciones=True)
        mensajeria.publicar(canal, calificacion_id, evento)
        conexion_rabbit.close()
    except pika.exceptions.AMQPError:
        log.error("calificación %s guardada, pero el evento se PERDIÓ: RabbitMQ no disponible", calificacion_id)
        return JSONResponse({"calificacionId": calificacion_id,
                             "error": "La calificación se guardó, pero el evento no se pudo publicar."},
                            status_code=500)
    return JSONResponse({"calificacionId": calificacion_id}, status_code=202)
