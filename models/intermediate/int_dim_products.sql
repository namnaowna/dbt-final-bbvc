{{
    config(
        materialized='incremental'
    )
}}

WITH dim_product AS
(
    SELECT ROW_NUMBER() OVER (ORDER BY category, price) AS product_id,
        category,
        item,
        price,
        DATETIME(TIMESTAMP(CURRENT_TIMESTAMP()), "Asia/Bangkok") AS update_at
    FROM {{ ref('stg_retail_store_cleaned') }}
    GROUP BY category, item, price
)
SELECT *
FROM dim_product

{% if is_incremental() %}

where update_at >= (select update_at from {{ this }} )

{% endif %}
