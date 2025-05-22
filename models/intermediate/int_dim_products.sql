{{
    config(
        materialized='incremental',
        unique_key = ['category', 'item', 'price']
    )
}}

WITH dim_product AS
(
    SELECT ROW_NUMBER() OVER (ORDER BY category, price) AS product_id,
        category,
        item,
        price,
    FROM {{ ref('stg_retail_store_cleaned') }}
    GROUP BY category, item, price
)
, transaction_date AS
(
    SELECT *
    FROM {{ ref('stg_retail_store_cleaned') }}
)
SELECT 
    dp.product_id,
    dp.category,
    dp.item,
    dp.price,
    MAX(td.transaction_date) AS transaction_date,
    CURRENT_DATETIME("Asia/Bangkok") AS update_at
FROM dim_product dp
LEFT JOIN transaction_date td 
    ON dp.category = td.category 
    AND dp.item = td.item 
    AND dp.price = td.price
GROUP BY dp.product_id, dp.category, dp.item, dp.price

{% if is_incremental() %}

HAVING transaction_date > (SELECT MAX(transaction_date) FROM {{ this }} )

{% endif %} 