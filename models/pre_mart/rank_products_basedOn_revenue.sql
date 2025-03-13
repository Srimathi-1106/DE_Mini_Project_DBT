SELECT
    product_id,
    product_family,
    sum(total_revenue) AS revenue,
    DENSE_RANK() OVER(ORDER BY revenue DESC) as rank_number
FROM
    {{  ref('int_full') }}
GROUP BY
    product_id,
    product_family