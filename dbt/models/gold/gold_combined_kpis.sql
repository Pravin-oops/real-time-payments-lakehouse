-- Gold: Unified financial KPI dashboard metrics
{{ config(materialized='table', tags=['gold', 'kpi']) }}

WITH upi_kpis AS (
    SELECT
        DATE_TRUNC('hour', txn_hour) AS metric_hour,
        'UPI'                         AS payment_type,
        SUM(total_transactions)       AS txn_count,
        SUM(total_volume_inr) / 83.0  AS volume_usd,   -- approx INR→USD
        AVG(failure_rate_pct)         AS failure_rate
    FROM {{ ref('gold_upi_hourly') }}
    GROUP BY 1, 2
),
cc_kpis AS (
    SELECT
        DATE_TRUNC('hour', swipe_hour) AS metric_hour,
        'CREDIT_CARD'                   AS payment_type,
        SUM(total_swipes)               AS txn_count,
        SUM(total_volume_usd)           AS volume_usd,
        AVG(fraud_rate_pct)             AS failure_rate
    FROM {{ ref('gold_cc_hourly') }}
    GROUP BY 1, 2
)
SELECT * FROM upi_kpis
UNION ALL
SELECT * FROM cc_kpis
ORDER BY metric_hour DESC, payment_type