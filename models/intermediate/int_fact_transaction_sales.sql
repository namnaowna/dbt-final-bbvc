{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

WITH fact_transaction_sales AS
(
    SELECT ft.transaction_id,
        ft.customer_id,
        dp.product_id,
        ft.quantity,
        ft.total_spent,
        ft.payment_method,
        ft.location,
        ft.transaction_date,
        CURRENT_DATETIME("Asia/Bangkok") AS updated_at
    FROM {{ ref('stg_retail_store_cleaned') }} ft 
        LEFT JOIN {{ ref('int_dim_products') }} dp ON ft.category = dp.category AND ft.item = dp.item AND ft.price = dp.price
)
SELECT
    ft.transaction_id,
    ft.customer_id,
    ft.product_id,
    ft.quantity,
    ft.total_spent,
    ft.payment_method,
    ft.location,
    ft.transaction_date,
    {% if is_incremental() %}
        CASE
            -- t คือตารางเดิม / ft คือตารางที่มีข้อมูลใหม่
            WHEN t.transaction_id IS NULL THEN CURRENT_DATETIME("Asia/Bangkok") -- กรณี insert ใหม่
            WHEN ft.customer_id     != t.customer_id
                OR ft.product_id      != t.product_id
                OR ft.quantity        != t.quantity
                OR ft.total_spent     != t.total_spent
                OR ft.payment_method  != t.payment_method
                OR ft.location        != t.location
                OR ft.transaction_date!= t.transaction_date
            THEN CURRENT_DATETIME("Asia/Bangkok")
            ELSE t.updated_at
        END AS updated_at
    {% else %}
        CURRENT_DATETIME("Asia/Bangkok") AS updated_at
    {% endif %}
FROM fact_transaction_sales ft
{% if is_incremental() %}
LEFT JOIN {{ this }} t
    ON ft.transaction_id = t.transaction_id
{% endif %}