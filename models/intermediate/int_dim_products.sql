WITH dim_product AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['category', 'item']) }} AS product_id,
        category,
        item,
        MAX(price) AS price
    FROM {{ ref('stg_retail_store_cleaned') }}
    GROUP BY category, item
)

SELECT 
    dp.product_id,
    dp.category,
    dp.item,
    dp.price,
    {% if is_incremental() %}
        CASE
            WHEN t.product_id IS NULL THEN CURRENT_DATETIME("Asia/Bangkok")
            WHEN dp.category != t.category
                OR dp.item != t.item
                OR dp.price != t.price
            THEN CURRENT_DATETIME("Asia/Bangkok")
            ELSE t.updated_at
        END AS updated_at
    {% else %}
        CURRENT_DATETIME("Asia/Bangkok") AS updated_at
    {% endif %}
FROM dim_product dp
{% if is_incremental() %}
LEFT JOIN {{ this }} t 
    ON dp.product_id = t.product_id
{% endif %}