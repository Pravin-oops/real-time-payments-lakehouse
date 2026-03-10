CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS silver.upi_transactions (
    txn_id             TEXT PRIMARY KEY,
    event_timestamp    TIMESTAMPTZ,
    sender_upi         TEXT,
    receiver_upi       TEXT,
    sender_bank        TEXT,
    receiver_bank      TEXT,
    amount_inr         NUMERIC(15,2),
    currency           TEXT DEFAULT 'INR',
    status             TEXT,
    category           TEXT,
    device_type        TEXT,
    city               TEXT,
    state              TEXT,
    is_failed          BOOLEAN DEFAULT FALSE,
    is_large_txn       BOOLEAN DEFAULT FALSE,
    txn_hash           TEXT,
    batch_date         DATE,
    _silver_timestamp  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.cc_swipes (
    event_id           TEXT PRIMARY KEY,
    event_time         TIMESTAMPTZ,
    card_token         TEXT,
    card_type          TEXT,
    last_four          TEXT,
    merchant_name      TEXT,
    merchant_id        TEXT,
    mcc                TEXT,
    mcc_description    TEXT,
    amount_usd         NUMERIC(12,2),
    currency           TEXT DEFAULT 'USD',
    pos_entry_mode     TEXT,
    country            TEXT,
    city               TEXT,
    approval_code      TEXT,
    response_code      TEXT,
    is_fraud_flagged   BOOLEAN DEFAULT FALSE,
    fraud_tier         TEXT,
    fraud_score        NUMERIC(6,4),
    txn_status         TEXT,
    network_latency_ms INTEGER,
    event_date         DATE,
    _silver_timestamp  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id         SERIAL PRIMARY KEY,
    pipeline_name  TEXT NOT NULL,
    layer          TEXT NOT NULL,
    status         TEXT NOT NULL,
    rows_processed BIGINT,
    started_at     TIMESTAMPTZ DEFAULT NOW(),
    completed_at   TIMESTAMPTZ,
    error_message  TEXT
);

CREATE INDEX IF NOT EXISTS idx_upi_event_ts  ON silver.upi_transactions(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_upi_bank      ON silver.upi_transactions(sender_bank);
CREATE INDEX IF NOT EXISTS idx_upi_category  ON silver.upi_transactions(category);
CREATE INDEX IF NOT EXISTS idx_cc_event_time ON silver.cc_swipes(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_cc_card_type  ON silver.cc_swipes(card_type);
CREATE INDEX IF NOT EXISTS idx_cc_fraud      ON silver.cc_swipes(is_fraud_flagged);