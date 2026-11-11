-- Esquema de la demo. Lo ejecuta PostgreSQL al crear la base (docker-entrypoint-initdb.d)
-- y también las pruebas, sobre su propia base. Por eso usa IF NOT EXISTS.

-- Datos de negocio: los viajes.
CREATE TABLE IF NOT EXISTS viajes (
    id                  UUID PRIMARY KEY,
    cliente_id          TEXT NOT NULL,
    origen              TEXT NOT NULL,
    destino             TEXT NOT NULL,
    estado              TEXT NOT NULL DEFAULT 'SOLICITADO',
    conductor           TEXT,
    -- Cuántas veces el worker aplicó la asignación. Con un consumidor idempotente
    -- tiene que valer 1 aunque el mismo evento llegue varias veces.
    asignaciones        INT NOT NULL DEFAULT 0,
    creado_en           TIMESTAMPTZ NOT NULL DEFAULT now(),
    actualizado_en      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Bandeja de salida (Transactional Outbox): eventos que el relay tiene que publicar.
-- Se escribe en la MISMA transacción que la tabla viajes.
CREATE TABLE IF NOT EXISTS outbox (
    id              UUID PRIMARY KEY,               -- eventoId: el consumidor deduplica con este valor
    tipo            TEXT NOT NULL,                  -- por ejemplo, ViajeSolicitado
    payload         JSONB NOT NULL,
    estado          TEXT NOT NULL DEFAULT 'PENDIENTE' CHECK (estado IN ('PENDIENTE', 'PUBLICADO')),
    creado_en       TIMESTAMPTZ NOT NULL DEFAULT now(),
    publicado_en    TIMESTAMPTZ
);

-- Índice parcial: el relay solo recorre los pendientes aunque haya miles de publicados.
CREATE INDEX IF NOT EXISTS outbox_pendientes ON outbox (creado_en) WHERE estado = 'PENDIENTE';

-- Eventos que el worker ya procesó (consumidor idempotente).
CREATE TABLE IF NOT EXISTS eventos_procesados (
    evento_id       UUID PRIMARY KEY,
    procesado_en    TIMESTAMPTZ NOT NULL DEFAULT now()
);
