-- dbt/models/staging/stg_customers.sql
WITH source AS (
    SELECT * FROM {{ source('source_db', 'customers') }}
)

SELECT
    customer_id,
    name,
    email,
    phone,
    registration_date
FROM source