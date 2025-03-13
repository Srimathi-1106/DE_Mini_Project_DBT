WITH first_purchase AS (
    SELECT
        customer_id,
        customer_name,
        MIN(payment_month) AS first_purchase_date
    FROM
        {{ ref('int_full') }}
    GROUP BY customer_id,customer_name
)

SELECT
    year(first_purchase_date) AS fiscal_year,
    customer_id,
    customer_name
FROM 
    first_purchase
ORDER BY fiscal_year,customer_id



