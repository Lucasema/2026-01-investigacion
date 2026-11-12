# Configuración por variables de entorno: el mismo código corre en Docker, en la PC
# o en las pruebas; solo cambian estos valores.
import logging
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://demo:demo@localhost:5672/%2F")

# Cola donde viajan los eventos ConductorCalificado
COLA = os.getenv("COLA", "calificaciones")

# Segundos que tarda el worker en procesar cada mensaje (simula un trabajo lento)
TIEMPO_PROCESAMIENTO = float(os.getenv("TIEMPO_PROCESAMIENTO", "3"))

# Cada cuántos segundos el relay revisa la tabla outbox
INTERVALO_RELAY = float(os.getenv("INTERVALO_RELAY", "1"))


def configurar_logs(nombre: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s")
    # pika registra cada conexión fallida con un traceback largo; nuestro código ya lo informa en una línea
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    return logging.getLogger(nombre)
