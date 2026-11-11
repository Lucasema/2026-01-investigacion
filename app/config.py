# Configuración por variables de entorno (12-Factor): el mismo código corre en Docker,
# en la PC o en las pruebas; solo cambian estos valores.
import logging
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

# Cola principal y su Dead Letter Queue
COLA_ASIGNACION = os.getenv("COLA_ASIGNACION", "asignacion.viaje-solicitado")
COLA_DLQ = f"{COLA_ASIGNACION}.dlq"

# Segundos que tarda el worker en "buscar un conductor" (simula la operación lenta)
TIEMPO_PROCESAMIENTO = float(os.getenv("TIEMPO_PROCESAMIENTO", "3"))

# Cada cuántos segundos el relay revisa la tabla outbox
INTERVALO_RELAY = float(os.getenv("INTERVALO_RELAY", "1"))


def configurar_logs(nombre: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    )
    # pika registra cada intento de conexión fallido como ERROR con traceback.
    # Nuestro código ya informa esos casos en una línea, así que se silencia.
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    return logging.getLogger(nombre)
