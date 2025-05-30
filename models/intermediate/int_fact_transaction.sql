{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}
WITH fact_transaction AS
(
    SELECT ft.transaction_id,
        ft.customer_id,
        dp.product_id,
        ft.quantity,
        ft.total_spent,
        ft.payment_method,
        ft.location,
        ft.transaction_date,
        ft.updated_at
    FROM {{ ref('stg_retail_store_cleaned') }} ft 
        LEFT JOIN {{ ref('int_dim_products') }} dp ON ft.category = dp.category AND ft.item = dp.item AND ft.price = dp.price
)
SELECT *
FROM fact_transaction
{% if is_incremental() %}

WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }} )

{% endif %} 