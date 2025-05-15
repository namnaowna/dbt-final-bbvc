WITH retail_store AS
(
    SELECT *
    FROM {{ ref('stg_retail_store') }}
)
, check_duplicate AS
(
    SELECT transaction_id,
        customer_id,
        category,
        item,
        price,
        quantity,
        total_spent,
        payment_method,
        location,
        transaction_date,
        discount_applied
    FROM retail_store
    GROUP BY transaction_id,
        customer_id,
        category,
        item,
        price,
        quantity,
        total_spent,
        payment_method,
        location,
        transaction_date,
        discount_applied
    HAVING COUNT(*) > 1
)
, check_other_duplicate AS
(
    SELECT customer_id,
        category,
        item,
        price,
        quantity,
        total_spent,
        payment_method,
        location,
        transaction_date,
        discount_applied
    FROM retail_store
    GROUP BY customer_id,
        category,
        item,
        price,
        quantity,
        total_spent,
        payment_method,
        location,
        transaction_date,
        discount_applied
    HAVING COUNT(*) > 1
)
, check_data_type AS
(
    SELECT SUM(quantity * price) AS total_spent_float,
        SUM(CAST(quantity AS BIGDECIMAL) * CAST(price AS BIGDECIMAL)) AS total_spent_bigdecimal
    FROM retail_store
)
-- have quantity and total_spent is null it suspicious
-- Item column is null it can be use price to have pattern
-- Price is null it can be use total_spent/quantity 
, check_price_quantity_total_is_null AS
(
    SELECT item, price, quantity, total_spent
    FROM retail_store
    WHERE price IS NULL OR quantity IS NULL OR total_spent IS NULL
    GROUP BY item, price, quantity, total_spent
)
, check_row_suspicious AS
-- 604 row
(
    SELECT *
    FROM retail_store
    WHERE quantity IS NULL AND total_spent IS NULL
)
, customer_format AS
(
    SELECT customer_id, REGEXP_CONTAINS(customer_id, r'^CUST_[0-9]+') AS valid_customer_item
    FROM retail_store
    GROUP BY customer_id
)
, check_valid_customer AS
(
    SELECT *
    FROM customer_format
    WHERE valid_customer_item = FALSE
)
, category_item_format AS
(
    SELECT category, item, REGEXP_CONTAINS(item, r'^Item_([1-9]|1[0-9]|2[0-5])_(BEV|BUT|CEA|EHE|FOOD|FUR|MILK|PAT)$') AS valid_category_item
    FROM retail_store
    GROUP BY category, item
)
, check_valid_category_item AS
(
    SELECT *
    FROM category_item_format
    WHERE valid_category_item = FALSE
)
, check_positive_value AS
(
    SELECT price, quantity, total_spent
    FROM retail_store
    WHERE price < 0 OR quantity < 0 OR total_spent < 0
)
, item_price_pattern AS 
(
  SELECT 
    item,
    price,
    {{ item_price_pattern('CAST(REGEXP_EXTRACT(item, r"^Item_(\\d+)_") AS INT64)') }} AS price_pattern
  FROM retail_store
)
, check_valid_item_price_pattern AS 
(
    SELECT *
    FROM item_price_pattern
    WHERE price != price_pattern
)
, total_spent_is_correct AS
(
    SELECT price * quantity AS valid_total_spent
        , total_spent
    FROM retail_store
)
, check_valid_total_spent AS
(
    SELECT *
    FROM total_spent_is_correct
    WHERE valid_total_spent != total_spent
)
SELECT *
FROM check_valid_item_price_pattern