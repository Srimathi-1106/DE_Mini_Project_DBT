SELECT 
    "customer id"::INTEGER AS customer_id,
    UPPER(TRIM(customername)) AS customer_name,
    company as company_name
    
FROM {{ source('raw', 'raw_customers') }}
WHERE customer_id IS NOT NULL
