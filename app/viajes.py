# Lógica de negocio de la API: registrar un viaje.
# Está separada de los endpoints para poder probarla directamente.
import uuid

import psycopg
from psycopg.types.json import Jsonb

from app import config


def conectar_db() -> psycopg.Connection:
    return psycopg.connect(config.DATABASE_URL, connect_timeout=3)


def solicitar_viaje_con_outbox(cliente_id: str, origen: str, destino: str,
                               evento_id: str | None = None) -> tuple[str, str]:
    """Transactional Outbox: guarda el viaje y el evento ViajeSolicitado en UNA transacción.

    No se conecta a RabbitMQ. Si cualquiera de los dos INSERT falla, PostgreSQL deshace
    los dos (ROLLBACK): nunca queda un viaje sin evento ni un evento sin viaje.
    Devuelve (viaje_id, evento_id).
    """
    viaje_id = str(uuid.uuid4())
    evento_id = evento_id or str(uuid.uuid4())
    payload = {"viajeId": viaje_id, "clienteId": cliente_id, "origen": origen, "destino": destino}

    # psycopg abre la transacción con el primer comando (BEGIN implícito).
    # Al salir del bloque "with" sin errores hace COMMIT; si hay una excepción, ROLLBACK.
    with conectar_db() as conexion:
        conexion.execute(
            "INSERT INTO viajes (id, cliente_id, origen, destino) VALUES (%s, %s, %s, %s)",
            (viaje_id, cliente_id, origen, destino),
        )
        conexion.execute(
            "INSERT INTO outbox (id, tipo, payload) VALUES (%s, 'ViajeSolicitado', %s)",
            (evento_id, Jsonb(payload)),
        )
    return viaje_id, evento_id


def guardar_viaje_sin_outbox(cliente_id: str, origen: str, destino: str) -> str:
    """Primera mitad de la versión SIN outbox: guarda solo el viaje (COMMIT inmediato)."""
    viaje_id = str(uuid.uuid4())
    with conectar_db() as conexion:
        conexion.execute(
            "INSERT INTO viajes (id, cliente_id, origen, destino) VALUES (%s, %s, %s, %s)",
            (viaje_id, cliente_id, origen, destino),
        )
    return viaje_id


def obtener_viaje(viaje_id: str) -> dict | None:
    with conectar_db() as conexion:
        fila = conexion.execute(
            "SELECT id, cliente_id, origen, destino, estado, conductor, asignaciones, "
            "creado_en, actualizado_en FROM viajes WHERE id = %s",
            (viaje_id,),
        ).fetchone()
    if fila is None:
        return None
    columnas = ["viajeId", "clienteId", "origen", "destino", "estado", "conductor",
                "asignaciones", "creadoEn", "actualizadoEn"]
    viaje = dict(zip(columnas, fila))
    viaje["viajeId"] = str(viaje["viajeId"])
    return viaje


def listar_outbox(estado: str | None = None, limite: int = 50) -> list[dict]:
    consulta = "SELECT id, tipo, payload, estado, creado_en, publicado_en FROM outbox"
    parametros: tuple = ()
    if estado:
        consulta += " WHERE estado = %s"
        parametros = (estado,)
    consulta += " ORDER BY creado_en DESC LIMIT %s"
    parametros += (limite,)
    with conectar_db() as conexion:
        filas = conexion.execute(consulta, parametros).fetchall()
    return [
        {"eventoId": str(f[0]), "tipo": f[1], "payload": f[2], "estado": f[3],
         "creadoEn": f[4], "publicadoEn": f[5]}
        for f in filas
    ]
