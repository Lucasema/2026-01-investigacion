# Preparación común de las pruebas.
#
# Las pruebas usan PostgreSQL y RabbitMQ reales (los del compose, con el perfil "test"),
# pero con una base (demo_test) y colas propias, así no tocan los datos de la demo.
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from app import config, mensajeria

ESQUEMA = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


@pytest.fixture(scope="session", autouse=True)
def base_de_prueba():
    """Crea la base de pruebas (si no existe) y le aplica el esquema."""
    nombre = conninfo_to_dict(config.DATABASE_URL)["dbname"]
    with psycopg.connect(make_conninfo(config.DATABASE_URL, dbname="postgres"), autocommit=True) as admin:
        existe = admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (nombre,)).fetchone()
        if not existe:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(nombre)))
    with psycopg.connect(config.DATABASE_URL) as conexion:
        conexion.execute(ESQUEMA.read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def entorno_limpio():
    """Cada prueba arranca con las tablas y las colas vacías."""
    with psycopg.connect(config.DATABASE_URL) as conexion:
        conexion.execute("TRUNCATE viajes, outbox, eventos_procesados")
    conexion_rabbit, canal = mensajeria.conectar()
    canal.queue_purge(config.COLA_ASIGNACION)
    canal.queue_purge(config.COLA_DLQ)
    conexion_rabbit.close()
    yield


@pytest.fixture
def canal():
    """Canal de RabbitMQ con publisher confirms, como el que usa el relay."""
    conexion, canal = mensajeria.conectar(confirmar_publicaciones=True)
    yield canal
    if conexion.is_open:
        conexion.close()


def consultar(sql_texto: str, parametros: tuple = ()):
    with psycopg.connect(config.DATABASE_URL) as conexion:
        return conexion.execute(sql_texto, parametros).fetchall()


def mensajes_en_cola(canal, cola: str) -> int:
    """Cantidad de mensajes listos (Ready) en una cola."""
    return canal.queue_declare(queue=cola, passive=True).method.message_count
