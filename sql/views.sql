-- Create view on transformations_clean which contains cleaned and ready data

CREATE OR REPLACE VIEW hourly_metrics AS
SELECT
    event_hour AS hour,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM transactions_clean
GROUP BY event_hour;
