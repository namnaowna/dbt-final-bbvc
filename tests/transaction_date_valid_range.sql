SELECT transaction_date
FROM {{ ref('stg_retail_store') }}
WHERE transaction_date < '2022-01-01' OR transaction_date > CURRENT_DATE()