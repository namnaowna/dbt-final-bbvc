WITH mart_sales AS
(
    SELECT ft.transaction_id,
        ft.total_spent,
        ft.quantity,
        dp.category,
        dp.item,
        ft.location,
        ft.transaction_date,
        -- Sunday(1) Monday(2) Tuesday(3) Wednesday(4) Thursday(5) Friday(6) Saturday(7) 
        EXTRACT(DAYOFWEEK FROM ft.transaction_date) AS week_day,
        EXTRACT(YEAR FROM ft.transaction_date) AS year,
        EXTRACT(MONTH FROM ft.transaction_date) AS month
    FROM {{ ref("int_fact_transaction")}} ft
        LEFT JOIN {{ ref("int_dim_products") }} dp ON ft.product_id = dp.product_id
)
SELECT *
FROM mart_sales
WHERE quantity IS NOT NULL