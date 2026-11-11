# API de Viajes (productor).
#
# POST /viajes             -> CON outbox: guarda viaje + evento en una transacción y responde 202.
# POST /viajes/sin-outbox  -> SIN outbox: guarda el viaje y publica directo en RabbitMQ
#                             (la versión ingenua, para comparar en la demo).
# GET  /viajes/{id}        -> estado actual del viaje (lo consulta el cliente después del 202).
# GET  /outbox             -> eventos de la bandeja de salida (para observar la demo).
# GET  /health             -> estado de la API y de la base.
import json
from uuid import UUID

import pika
import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app import config, mensajeria, viajes

log = config.configurar_logs("api")

app = FastAPI(title="Demo colas + Transactional Outbox", version="1.0.0")


class SolicitudViaje(BaseModel):
    clienteId: str = Field(min_length=1, examples=["ana-gomez"])
    origen: str = Field(min_length=1, examples=["Av. Sarmiento 1500"])
    destino: str = Field(min_length=1, examples=["French 414"])


@app.post("/viajes", status_code=202)
def solicitar_viaje(datos: SolicitudViaje):
    """Versión CON Transactional Outbox. No toca RabbitMQ: funciona aunque el broker esté caído."""
    try:
        viaje_id, evento_id = viajes.solicitar_viaje_con_outbox(datos.clienteId, datos.origen, datos.destino)
    except psycopg.OperationalError:
        raise HTTPException(503, "La base de datos no está disponible.")
    log.info("viaje=%s evento=%s guardados en una transacción (outbox PENDIENTE)", viaje_id, evento_id)
    # 202 Accepted: recibido y registrado; la asignación del conductor se hace después
    return JSONResponse(
        {"viajeId": viaje_id, "estado": "SOLICITADO", "eventoId": evento_id},
        status_code=202,
        headers={"Location": f"/viajes/{viaje_id}"},
    )


@app.post("/viajes/sin-outbox", status_code=202)
def solicitar_viaje_sin_outbox(datos: SolicitudViaje):
    """Versión SIN outbox (escritura dual). Sirve para mostrar el problema.

    1. Guarda el viaje (COMMIT).
    2. Publica en RabbitMQ.
    Si el paso 2 falla, el viaje ya quedó guardado y el mensaje se perdió para siempre.
    """
    viaje_id = viajes.guardar_viaje_sin_outbox(datos.clienteId, datos.origen, datos.destino)
    try:
        conexion, canal = mensajeria.conectar()
        try:
            canal.basic_publish(
                exchange="",
                routing_key=config.COLA_ASIGNACION,
                body=json.dumps({"eventoId": viaje_id, "viajeId": viaje_id}),
                properties=mensajeria.propiedades_persistentes(viaje_id),
            )
        finally:
            conexion.close()
    except pika.exceptions.AMQPError as error:
        log.error("viaje=%s guardado pero el evento se PERDIÓ (RabbitMQ no disponible: %s)",
                  viaje_id, error.__class__.__name__)
        return JSONResponse(
            {"viajeId": viaje_id, "estado": "SOLICITADO",
             "error": "El viaje se guardó pero el evento no se pudo publicar: nadie lo va a procesar."},
            status_code=500,
        )
    log.info("viaje=%s guardado y publicado directo (sin outbox)", viaje_id)
    return JSONResponse({"viajeId": viaje_id, "estado": "SOLICITADO"}, status_code=202,
                        headers={"Location": f"/viajes/{viaje_id}"})


@app.get("/viajes/{viaje_id}")
def consultar_viaje(viaje_id: UUID):
    viaje = viajes.obtener_viaje(str(viaje_id))
    if viaje is None:
        raise HTTPException(404, "El viaje no existe.")
    return viaje


@app.get("/outbox")
def consultar_outbox(estado: str | None = None, limite: int = 50):
    return viajes.listar_outbox(estado, limite)


@app.get("/health")
def salud():
    try:
        with viajes.conectar_db() as conexion:
            conexion.execute("SELECT 1")
        return {"estado": "ok", "base": "ok"}
    except psycopg.OperationalError:
        return JSONResponse({"estado": "degradado", "base": "no disponible"}, status_code=503)
