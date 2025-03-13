SELECT
    customer_id,
    customer_name,
    sum(total_revenue) as revenue,
    DENSE_RANK() OVER(ORDER BY revenue DESC) AS rank_number
FROM
    {{ ref('int_full') }}
GROUP BY 
    customer_id,
    customer_name
