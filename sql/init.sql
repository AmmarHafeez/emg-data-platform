CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.emg_signals (
    subject_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    channel_1 DOUBLE PRECISION NOT NULL,
    channel_2 DOUBLE PRECISION NOT NULL,
    channel_3 DOUBLE PRECISION NOT NULL,
    channel_4 DOUBLE PRECISION NOT NULL,
    channel_5 DOUBLE PRECISION NOT NULL,
    channel_6 DOUBLE PRECISION NOT NULL,
    channel_7 DOUBLE PRECISION NOT NULL,
    channel_8 DOUBLE PRECISION NOT NULL,
    gesture_label TEXT NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_emg_signals_subject_id
    ON raw.emg_signals (subject_id);

CREATE INDEX IF NOT EXISTS idx_emg_signals_session_id
    ON raw.emg_signals (session_id);

CREATE INDEX IF NOT EXISTS idx_emg_signals_timestamp
    ON raw.emg_signals (timestamp);

CREATE INDEX IF NOT EXISTS idx_emg_signals_gesture_label
    ON raw.emg_signals (gesture_label);
