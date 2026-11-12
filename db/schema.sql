-- Tablas de la demo. PostgreSQL las crea al iniciar (docker-entrypoint-initdb.d);
-- las pruebas también las crean en su propia base, por eso usan IF NOT EXISTS.

-- ===== Lado de M2 (Clientes): la calificación y su bandeja de salida =====

-- Calificaciones que los clientes le ponen al conductor después de un viaje (RF-2.5).
CREATE TABLE IF NOT EXISTS calificaciones (
    id              UUID PRIMARY KEY,
    viaje_id        TEXT NOT NULL,
    conductor_id    TEXT NOT NULL,
    puntaje         INT NOT NULL CHECK (puntaje BETWEEN 1 AND 5),
    creado_en       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional Outbox: eventos que el relay tiene que publicar en RabbitMQ.
-- Se escribe en la MISMA transacción que la tabla calificaciones.
CREATE TABLE IF NOT EXISTS outbox (
    id              UUID PRIMARY KEY,               -- eventoId: con este valor el worker detecta duplicados
    tipo            TEXT NOT NULL,                  -- ConductorCalificado
    payload         JSONB NOT NULL,
    estado          TEXT NOT NULL DEFAULT 'PENDIENTE',  -- PENDIENTE o PUBLICADO
    creado_en       TIMESTAMPTZ NOT NULL DEFAULT now(),
    publicado_en    TIMESTAMPTZ
);

-- ===== Lado de M3 (Conductores): en la realidad sería otra base de datos =====

-- Promedio de cada conductor: suma de puntajes y cantidad de calificaciones contadas.
CREATE TABLE IF NOT EXISTS promedios_conductor (
    conductor_id    TEXT PRIMARY KEY,
    cantidad        INT NOT NULL,
    suma            INT NOT NULL
);

-- Eventos que el worker ya procesó: así un evento repetido no se cuenta dos veces.
CREATE TABLE IF NOT EXISTS eventos_procesados (
    evento_id       UUID PRIMARY KEY,
    procesado_en    TIMESTAMPTZ NOT NULL DEFAULT now()
);
