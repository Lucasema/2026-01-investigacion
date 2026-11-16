CREATE TABLE calificaciones (
    id SERIAL PRIMARY KEY,
    viaje_id VARCHAR(50) NOT NULL,
    cliente_id VARCHAR(50) NOT NULL,
    conductor_id VARCHAR(50) NOT NULL,
    puntaje INTEGER NOT NULL CHECK (puntaje BETWEEN 1 AND 5),
    comentario TEXT,
    estado VARCHAR(30) NOT NULL,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE outbox_eventos (
    id SERIAL PRIMARY KEY,
    calificacion_id INTEGER NOT NULL,
    tipo_evento VARCHAR(50) NOT NULL,
    payload TEXT NOT NULL,
    estado VARCHAR(30) NOT NULL,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);