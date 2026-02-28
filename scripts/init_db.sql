-- ============================================================
--  LogiStream – Initialisation de la base de données
--  Données synthétiques France
-- ============================================================

-- ── Référentiel routes ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS routes (
    route_id        VARCHAR(100) PRIMARY KEY,
    origin          VARCHAR(200),
    destination     VARCHAR(200),
    distance_km     FLOAT,
    avg_duration_h  FLOAT,
    risk_score      FLOAT DEFAULT 0.0,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ── Référentiel hubs ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hubs (
    hub_id      VARCHAR(100) PRIMARY KEY,
    hub_name    VARCHAR(200),
    city        VARCHAR(100),
    latitude    FLOAT,
    longitude   FLOAT,
    capacity    INTEGER DEFAULT 1000
);

-- ── Référentiel clients B2B ─────────────────────────────────
CREATE TABLE IF NOT EXISTS clients (
    client_id   VARCHAR(100) PRIMARY KEY,
    client_name VARCHAR(200),
    sla_hours   FLOAT DEFAULT 24.0,
    priority    INTEGER DEFAULT 1
);

-- ── Store d'état des colis (Golden Record) ──────────────────
CREATE TABLE IF NOT EXISTS parcel_state (
    parcel_id           VARCHAR(200) PRIMARY KEY,
    client_id           VARCHAR(100),
    route_id            VARCHAR(100),
    hub_id              VARCHAR(100),
    status              VARCHAR(50) DEFAULT 'planned',
    planned_pickup      TIMESTAMP,
    actual_pickup       TIMESTAMP,
    planned_delivery    TIMESTAMP,
    actual_delivery     TIMESTAMP,
    delay_minutes       FLOAT DEFAULT 0,
    risk_level          VARCHAR(20) DEFAULT 'low',
    risk_probability    FLOAT DEFAULT 0.0,
    weather_condition   VARCHAR(100),
    congestion_level    FLOAT DEFAULT 0.0,
    distance_km         FLOAT DEFAULT 0,
    origin_lat          FLOAT,
    origin_lon          FLOAT,
    dest_lat            FLOAT,
    dest_lon            FLOAT,
    current_lat         FLOAT,
    current_lon         FLOAT,
    vehicle_type        VARCHAR(100),
    region              VARCHAR(100),
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- ── Alertes ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    alert_id        SERIAL PRIMARY KEY,
    parcel_id       VARCHAR(200),
    alert_type      VARCHAR(50),
    severity        VARCHAR(20),
    message         TEXT,
    acknowledged    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ── Historique événements (time-series light) ────────────────
CREATE TABLE IF NOT EXISTS delivery_events_log (
    id              SERIAL PRIMARY KEY,
    parcel_id       VARCHAR(200),
    event_type      VARCHAR(50),
    event_timestamp TIMESTAMP,
    hub_id          VARCHAR(100),
    latitude        FLOAT,
    longitude       FLOAT,
    raw_payload     JSONB,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ── Error log (Dead Letter Queue) ───────────────────────────
CREATE TABLE IF NOT EXISTS error_logs (
    id              SERIAL PRIMARY KEY,
    topic           VARCHAR(100),
    raw_message     TEXT,
    error_reason    TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ── Index de performance ─────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_parcel_status       ON parcel_state(status);
CREATE INDEX IF NOT EXISTS idx_parcel_risk         ON parcel_state(risk_level);
CREATE INDEX IF NOT EXISTS idx_parcel_hub          ON parcel_state(hub_id);
CREATE INDEX IF NOT EXISTS idx_parcel_region       ON parcel_state(region);
CREATE INDEX IF NOT EXISTS idx_alerts_severity     ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_ack          ON alerts(acknowledged);
CREATE INDEX IF NOT EXISTS idx_events_parcel       ON delivery_events_log(parcel_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp    ON delivery_events_log(event_timestamp);

-- ── Données de seed ────────────────────────────────────────────
INSERT INTO hubs (hub_id, hub_name, city, latitude, longitude, capacity) VALUES
    ('HUB_PARIS',       'Paris CDG',       'Paris',       48.8566,  2.3522, 5000),
    ('HUB_LYON',        'Lyon Sud',        'Lyon',        45.7640,  4.8357, 3000),
    ('HUB_MARSEILLE',   'Marseille Fos',   'Marseille',   43.2965,  5.3698, 2500),
    ('HUB_BORDEAUX',    'Bordeaux Nord',   'Bordeaux',    44.8378, -0.5792, 2000),
    ('HUB_LILLE',       'Lille Nord',      'Lille',       50.6292,  3.0573, 2000),
    ('HUB_NANTES',      'Nantes Atlantique','Nantes',     47.2184, -1.5536, 1800),
    ('HUB_STRASBOURG',  'Strasbourg Est',  'Strasbourg',  48.5734,  7.7521, 1500)
ON CONFLICT DO NOTHING;

INSERT INTO routes (route_id, origin, destination, distance_km, avg_duration_h) VALUES
    ('R_PAR_LYO', 'Paris',     'Lyon',        465, 4.5),
    ('R_PAR_MAR', 'Paris',     'Marseille',   775, 7.5),
    ('R_PAR_BOR', 'Paris',     'Bordeaux',    585, 6.0),
    ('R_PAR_LIL', 'Paris',     'Lille',       225, 2.5),
    ('R_LYO_MAR', 'Lyon',      'Marseille',   315, 3.5),
    ('R_PAR_NAN', 'Paris',     'Nantes',      385, 4.0),
    ('R_PAR_STR', 'Paris',     'Strasbourg',  490, 5.0)
ON CONFLICT DO NOTHING;

INSERT INTO clients (client_id, client_name, sla_hours, priority) VALUES
    ('CLIENT_A', 'Carrefour Logistique', 24.0, 1),
    ('CLIENT_B', 'Amazon FR',           12.0, 1),
    ('CLIENT_C', 'Leroy Merlin',        48.0, 2),
    ('CLIENT_D', 'Decathlon Express',   36.0, 2),
    ('CLIENT_E', 'FNAC Direct',         24.0, 1)
ON CONFLICT DO NOTHING;
