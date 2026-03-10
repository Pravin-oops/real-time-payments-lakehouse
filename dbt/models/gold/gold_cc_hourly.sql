-- Gold: Credit card swipe aggregates by hour and card type
{{ config(materialized='table', tags=['gold', 'cc']) }}

SELECT
    DATE_TRUNC('hour', event_time)         AS swipe_hour,
    card_type,
    mcc_description,
    txn_status,
    COUNT(*)                                AS total_swipes,
    COUNT(*) FILTER (WHERE is_fraud_flagged) AS fraud_flagged_count,
    SUM(amount_usd)                         AS total_volume_usd,
    AVG(amount_usd)                         AS avg_swipe_amount,
    AVG(network_latency_ms)                 AS avg_latency_ms,
    COUNT(DISTINCT merchant_id)             AS unique_merchants,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_fraud_flagged) / NULLIF(COUNT(*), 0), 2
    )                                       AS fraud_rate_pct,
    CURRENT_TIMESTAMP                       AS dbt_updated_at
FROM {{ source('silver', 'cc_swipes') }}
WHERE event_time >= NOW() - INTERVAL '48 hours'
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, total_volume_usd DESC