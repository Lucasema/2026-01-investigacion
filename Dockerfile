# Una sola imagen para la API, el relay, el worker y las pruebas; cambia el comando.
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY app ./app
COPY scripts ./scripts
COPY tests ./tests
COPY db ./db
COPY pytest.ini ./

RUN useradd --create-home demo
USER demo

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
