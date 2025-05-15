WITH dim_product AS
(
    SELECT ROW_NUMBER() OVER (ORDER BY category, price) AS product_id,
        category,
        item,
        price
    FROM {{ ref('stg_retail_store_cleaned') }}
    GROUP BY category, item, price
)
SELECT *
FROM dim_product