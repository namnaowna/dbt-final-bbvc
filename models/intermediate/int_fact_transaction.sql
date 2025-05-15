WITH fact_transaction AS
(
    SELECT ft.transaction_id,
        ft.customer_id,
        dp.product_id,
        ft.quantity,
        ft.total_spent,
        ft.payment_method,
        ft.location,
        ft.transaction_date
    FROM {{ ref('stg_retail_store_cleaned') }} ft 
        LEFT JOIN {{ ref('int_dim_products') }} dp ON ft.category = dp.category AND ft.item = dp.item AND ft.price = dp.price
)
SELECT *
FROM fact_transaction