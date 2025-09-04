SELECT
    validFor,
    `order`,
    country,
    currency,
    amount,
    currencyCode,
    rate,
    load_date
FROM {{ source('cnbdb', 'stage_fx_rates') }}