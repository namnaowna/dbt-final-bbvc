{{
    config(
        materialized='incremental',
        unique_key='product_id',
    )
}}

WITH dim_product AS
(
    SELECT {{ dbt_utils.generate_surrogate_key(['category', 'item']) }} AS product_id,
        category,
        item,
        price,
        MAX(updated_at) AS updated_at
    FROM {{ ref('stg_retail_store_cleaned') }}
    GROUP BY category, item, price
)
SELECT *
FROM dim_product
{% if is_incremental() %}

WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }} )

{% endif %} 
