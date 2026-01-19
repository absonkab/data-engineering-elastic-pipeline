CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    user_id INTEGER,
    amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(3),
    status VARCHAR(20),
    event_time TIMESTAMP NOT NULL,
    processing_time TIMESTAMP NOT NULL,
    is_late_event BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_transactions_event_time
    ON transactions (event_time);
