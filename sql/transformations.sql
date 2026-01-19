-- Create clean table

DROP TABLE IF EXISTS transactions_clean;

CREATE TABLE transactions_clean AS
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

CREATE INDEX idx_transactions_clean_event_time
    ON transactions_clean(event_time);

CREATE INDEX idx_transactions_clean_user_id
    ON transactions_clean(user_id);

CREATE INDEX idx_transactions_clean_is_late
    ON transactions_clean(is_late);
