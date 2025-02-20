-- dbt/models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('source_db', 'orders') }}
)

SELECT
    order_id,
    customer_id,
    order_date,
    amount
FROM source