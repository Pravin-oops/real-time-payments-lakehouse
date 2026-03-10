-- Gold: UPI transaction aggregates by hour and bank
{{ config(materialized='table', tags=['gold', 'upi']) }}

SELECT
    DATE_TRUNC('hour', event_timestamp)   AS txn_hour,
    sender_bank,
    category,
    COUNT(*)                               AS total_transactions,
    COUNT(*) FILTER (WHERE is_failed)     AS failed_transactions,
    SUM(amount_inr)                        AS total_volume_inr,
    AVG(amount_inr)                        AS avg_txn_amount,
    MAX(amount_inr)                        AS max_txn_amount,
    COUNT(*) FILTER (WHERE is_large_txn)  AS large_txn_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_failed) / NULLIF(COUNT(*), 0), 2
    )                                      AS failure_rate_pct,
    CURRENT_TIMESTAMP                      AS dbt_updated_at
FROM {{ source('silver', 'upi_transactions') }}
WHERE event_timestamp >= NOW() - INTERVAL '48 hours'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, total_volume_inr DESC