-- KPI 1: Number of transactions per hour
SELECT
    date_trunc('hour', event_time) AS hour,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY 1
ORDER BY 1;

-- KPI 2: Revenue per hour
SELECT
    date_trunc('hour', event_time) AS hour,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY 1
ORDER BY 1;

-- KPI 3: Top users by total spent
SELECT
    user_id,
    COUNT(*) AS nb_transactions,
    SUM(amount) AS total_spent
FROM transactions
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;

-- KPI 4: Late events detection (>2 minutes)
SELECT
    COUNT(*) AS late_events
FROM transactions
WHERE processing_time - event_time > INTERVAL '2 minutes';

SELECT
    transaction_id,
    event_time,
    processing_time,
    processing_time - event_time AS delay
FROM transactions
WHERE processing_time - event_time > INTERVAL '2 minutes'
ORDER BY delay DESC;

-- KPI 5: Rolling average of transaction amounts
SELECT
    event_time,
    amount,
    AVG(amount) OVER (
        ORDER BY event_time
        ROWS BETWEEN 500 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_5
FROM transactions
ORDER BY event_time;

-- Verify transformed table transformations_clean

SELECT COUNT(*) FROM transactions_clean;

SELECT is_late, COUNT(*)
FROM transactions_clean
GROUP BY is_late;

SELECT *
FROM transactions_clean
ORDER BY latency_seconds DESC
LIMIT 5;

-- Verify view table hourly_metrics

SELECT *
FROM hourly_metrics
ORDER BY hour DESC
LIMIT 5;

SELECT
    SUM(transaction_count),
    (SELECT COUNT(*) FROM transactions_clean)
FROM hourly_metrics;

SELECT *
FROM hourly_metrics
WHERE transaction_count = 0;
