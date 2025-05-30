{{
    config(
        materialized='incremental',
        unique_key = ['category', 'item', 'price'],
        merge_exclude_columns = ['update_at'],
        incremental_strategy = 'merge',
        merge_delete_rows = True
    )
}}

WITH dim_product AS
(
    SELECT ROW_NUMBER() OVER (ORDER BY category, price) AS product_id,
        category,
        item,
        price,
        CURRENT_DATETIME("Asia/Bangkok") AS update_at
    FROM {{ ref('stg_retail_store_cleaned') }}
    GROUP BY category, item, price
)
SELECT *
FROM dim_product

{% if is_incremental() %}

WHERE update_at > (SELECT MAX(update_at) FROM {{ this }} )

{% endif %} 