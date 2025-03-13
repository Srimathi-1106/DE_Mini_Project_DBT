SELECT 
    customer_id::INTEGER AS customer_id,
    product_id AS product_id,
    TO_DATE(payment_month, 'DD-MM-YYYY') AS payment_month, 
    revenue_type::INTEGER AS revenue_type,
    revenue::FLOAT AS revenue,
    quantity::INTEGER AS quantity,
    "dimension 1" AS dimension_1,
    "dimension 2" AS dimension_2,
    "dimension 3" AS dimension_3,
    "dimension 4" AS dimension_4,
    "dimension 5" AS dimension_5,
    "dimension 6" AS dimension_6,
    "dimension 7" AS dimension_7,
    "dimension 8" AS dimension_8,
    "dimension 9" AS dimension_9,
    "dimension 10" AS dimension_10,
    companies AS companies
FROM {{ source('raw', 'raw_transactions') }}
WHERE
customer_id IS NOT NULL

