SELECT 
customer_id::INTEGER as customer_id,
country,
region
FROM
 {{ source('raw','raw_regions')}}
WHERE 
customer_id IS NOT NULL