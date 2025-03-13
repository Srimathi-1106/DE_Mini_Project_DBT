SELECT
    t.customer_id,
    c.customer_name,
    c.company_name,
    t.product_id,
    p.product_family,
    p.product_sub_family,
    t.payment_month,
    t.revenue_type,
    (t.revenue*t.quantity) AS total_revenue,
    r.country,
    r.region
FROM
{{ ref('stg_customers' )}} AS c
INNER JOIN
{{ ref('stg_transactions') }} AS t
ON
c.customer_id = t.customer_id
INNER JOIN
{{ ref('stg_regions') }} AS r
ON
r.customer_id = t.customer_id
INNER JOIN
{{ ref('stg_products') }} AS p
ON
p.product_id = t.product_id