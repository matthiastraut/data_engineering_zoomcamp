SELECT
    SUM(total_monthly_trips)
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE service_type = 'Green' AND revenue_month = '2019-10-01'