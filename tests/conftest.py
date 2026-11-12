# Preparación común de las pruebas: usan PostgreSQL y RabbitMQ reales, pero con su propia
# base (demo_test) y su propia cola, así no tocan los datos de la demo.
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from app import config, mensajeria

ESQUEMA = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


@pytest.fixture(scope="session", autouse=True)
def base_de_prueba():
    """Crea la base de pruebas si no existe y le aplica el esquema."""
    nombre = conninfo_to_dict(config.DATABASE_URL)["dbname"]
    with psycopg.connect(make_conninfo(config.DATABASE_URL, dbname="postgres"), autocommit=True) as admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (nombre,)).fetchone():
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(nombre)))
    with psycopg.connect(config.DATABASE_URL) as conexion:
        conexion.execute(ESQUEMA.read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def entorno_limpio():
    """Cada prueba empieza con las tablas y la cola vacías."""
    consultar("TRUNCATE calificaciones, outbox, promedios_conductor, eventos_procesados")
    conexion, canal = mensajeria.conectar()
    canal.queue_purge(config.COLA)
    conexion.close()


@pytest.fixture
def canal():
    """Canal con publisher confirms, como el que usa el relay."""
    conexion, canal = mensajeria.conectar(confirmar_publicaciones=True)
    yield canal
    if conexion.is_open:
        conexion.close()


def consultar(sql_texto: str, parametros: tuple = ()):
    with psycopg.connect(config.DATABASE_URL) as conexion:
        cursor = conexion.execute(sql_texto, parametros)
        return cursor.fetchall() if cursor.description else None


def mensajes_en_cola(canal) -> int:
    return canal.queue_declare(queue=config.COLA, passive=True).method.message_count
