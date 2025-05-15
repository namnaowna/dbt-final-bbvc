WITH stg_retail_store AS
(
    SELECT *
    FROM {{ source('raw','retail_store_sales') }}
)
SELECT `Transaction ID` AS transaction_id,
    `Customer ID` AS customer_id,
    Category AS category,
    Item AS item,
    `Price Per Unit` AS price,
    Quantity AS quantity,
    `Total Spent` AS total_spent,
    `Payment Method` AS payment_method,
    Location AS location,
    `Transaction Date` AS transaction_date,
    `Discount Applied` AS discount_applied
FROM stg_retail_store
