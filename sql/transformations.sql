BEGIN;

-- Create clean table

CREATE TABLE IF NOT EXISTS transactions_clean (
    transaction_id BIGINT,
    user_id BIGINT,
    amount NUMERIC,
    currency TEXT,
    status TEXT,
    event_time TIMESTAMP,
    processing_time TIMESTAMP,
    event_date DATE,
    event_hour TIMESTAMP,
    latency_seconds DOUBLE PRECISION,
    is_late BOOLEAN
);

-- Clear data
TRUNCATE TABLE transactions_clean;

-- Refill table
INSERT INTO transactions_clean 
SELECT
    transaction_id,
    user_id,
    amount,
    currency,
    status,
    event_time,
    processing_time,

    -- derived fields
    DATE(event_time) AS event_date,
    date_trunc('hour', event_time) AS event_hour,

    -- latency in seconds
    EXTRACT(EPOCH FROM (processing_time - event_time)) AS latency_seconds,

    -- late event flag
    CASE
        WHEN processing_time - event_time > INTERVAL '2 minutes'
        THEN TRUE
        ELSE FALSE
    END AS is_late

FROM transactions
WHERE
    event_time IS NOT NULL
    AND amount > 0;


-- Create index

CREATE INDEX IF NOT EXISTS idx_transactions_clean_event_time
    ON transactions_clean(event_time);

CREATE INDEX IF NOT EXISTS idx_transactions_clean_user_id
    ON transactions_clean(user_id);

CREATE INDEX IF NOT EXISTS idx_transactions_clean_is_late
    ON transactions_clean(is_late);

COMMIT;
