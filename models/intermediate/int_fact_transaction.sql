{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        merge_exclude_columns = ['update_at']
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
        CURRENT_DATETIME("Asia/Bangkok") AS update_at
    FROM {{ ref('stg_retail_store_cleaned') }} ft 
        LEFT JOIN {{ ref('int_dim_products') }} dp ON ft.category = dp.category AND ft.item = dp.item AND ft.price = dp.price
)
SELECT *
FROM fact_transaction

{% if is_incremental() %}

WHERE update_at >= (SELECT MAX(update_at) FROM {{ this }})

{% endif %}