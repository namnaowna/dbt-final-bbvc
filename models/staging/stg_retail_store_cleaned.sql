{{ config( 
  materialized = 'table', 
) }} 

WITH retail_std_name AS
(
  SELECT transaction_id,
        customer_id,
        category,
        CAST(price AS BIGDECIMAL) AS price,
        CAST(quantity AS BIGDECIMAL) AS quantity,
        CAST(total_spent AS BIGDECIMAL) AS total_spent,
        item,
        payment_method,
        location,
        transaction_date
  FROM {{ ref('stg_retail_store') }}
)
, row_quantity_total_spent_null AS
-- 604 row is suspicious
(
    SELECT transaction_id,
        customer_id,
        category,
        price,
        quantity,
        total_spent,
        item,
        payment_method,
        location,
        transaction_date
    FROM retail_std_name
    WHERE quantity IS NULL AND total_spent IS NULL
)
, clean_data AS
(
  SELECT transaction_id,
    customer_id,
    category,
    IFNULL(CASE
      WHEN price IS NULL
        THEN ROUND(total_spent/quantity, 2)
      ELSE price
    END, 0) AS price,
    IFNULL(CASE
            WHEN quantity IS NULL
              THEN ROUND(total_spent/price, 2)
            ELSE quantity
          END, 0) AS quantity,
    IFNULL( CASE 
              WHEN total_spent IS NULL
                THEN ROUND(quantity * price, 2)
              ELSE total_spent
            END, 0) AS total_spent,
    item,
    payment_method,
    location,
    transaction_date
  FROM retail_std_name
  WHERE transaction_id NOT IN ( SELECT transaction_id FROM row_quantity_total_spent_null)
)
, row_union AS
-- เก็บค่า suspicious ไว้
(
  SELECT *
  FROM clean_data
  UNION ALL
  SELECT *
  FROM row_quantity_total_spent_null
)
, format_item AS
(
    SELECT transaction_id,
    customer_id,
    category,
    price,
    quantity,
    total_spent,
    {{ clean_item_by_category('item', 'category', 'price', start_price = 5.0, step = 1.5) }} AS item,
    payment_method,
    location,
    transaction_date
    FROM row_union
)
SELECT *
FROM format_item 
