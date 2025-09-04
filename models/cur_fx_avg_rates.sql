{{ config(
    materialized='table'
) }}


SELECT
    s.country,
    s.currency,
    s.currencyCode,
    AVG(s.rate) AS avg_rate,
    DATE_SUB(CAST('{{ var("batch_dt") }}' AS DATE), INTERVAL 3 MONTH) AS period_start,
    DATE_SUB(CAST('{{ var("batch_dt") }}' AS DATE), INTERVAL 1 DAY) AS period_end,
    CAST('{{ var("batch_dt") }}' AS DATE) AS snapshot_date
FROM {{ ref("stage_fx_rates") }} s
WHERE s.validFor >= DATE_SUB(CAST('{{ var("batch_dt") }}' AS DATE), INTERVAL 3 MONTH)
GROUP BY s.country, s.currency, s.currencyCode