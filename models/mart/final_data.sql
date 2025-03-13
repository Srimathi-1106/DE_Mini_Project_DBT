WITH 
-- Identify last purchase per customer-product pair
last_purchase AS (
    SELECT 
        customer_id, 
        product_id, 
        MAX(payment_month) AS last_purchase_month
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id, product_id
),

-- Identify product churn by checking if a customer stopped buying a product
product_churn AS (
    SELECT 
        lp.customer_id, 
        lp.product_id, 
        COUNT(lp.product_id) AS churned_products
    FROM last_purchase lp
    LEFT JOIN {{ ref('stg_transactions') }} t 
        ON lp.customer_id = t.customer_id 
        AND lp.product_id = t.product_id
        AND t.payment_month > DATEADD(month, 6, lp.last_purchase_month)  -- Fixed DATEADD issue
    WHERE t.customer_id IS NULL
    GROUP BY lp.customer_id, lp.product_id
),

-- Identify new customers (first-time buyers)
new_customers AS (
    SELECT 
        customer_id, 
        MIN(payment_month) AS first_purchase_month
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
),

-- Net Revenue Retention (NRR) & Gross Revenue Retention (GRR)
revenue_metrics AS (
    SELECT 
        payment_month,
        SUM(CASE WHEN revenue_type = 1 THEN revenue * quantity ELSE 0 END) AS recurring_revenue,
        SUM(revenue * quantity) AS total_revenue,
        SUM(CASE WHEN revenue_type = 1 THEN revenue * quantity ELSE 0 END) 
        / NULLIF(LAG(SUM(CASE WHEN revenue_type = 1 THEN revenue * quantity ELSE 0 END)) 
                 OVER (ORDER BY payment_month), 0) AS NRR,
        SUM(revenue * quantity) 
        / NULLIF(LAG(SUM(revenue * quantity)) OVER (ORDER BY payment_month), 0) AS GRR
    FROM {{ ref('stg_transactions') }}
    GROUP BY payment_month
),

-- Revenue lost due to contraction (churned revenue)
contraction_revenue AS (
    SELECT 
        t.payment_month,
        SUM(t.revenue * t.quantity) AS lost_revenue
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN product_churn pc 
        ON t.customer_id = pc.customer_id 
        AND t.product_id = pc.product_id
    WHERE pc.customer_id IS NOT NULL
    GROUP BY t.payment_month
),

-- Identify new logos (first-time customers in a fiscal year)
new_logos AS (
    SELECT 
        customer_id, 
        DATE_TRUNC('YEAR', first_purchase_month) AS fiscal_year
    FROM new_customers
),

-- Rank products based on revenue
ranked_products AS (
    SELECT 
        product_id, 
        SUM(revenue * quantity) AS product_revenue,
        RANK() OVER (ORDER BY SUM(revenue * quantity) DESC) AS product_rank
    FROM {{ ref('stg_transactions') }}
    GROUP BY product_id
),

-- Rank customers based on revenue
ranked_customers AS (
    SELECT 
        customer_id, 
        SUM(revenue * quantity) AS customer_revenue,
        RANK() OVER (ORDER BY SUM(revenue * quantity) DESC) AS customer_rank
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
)

-- Final aggregated table
SELECT 
    t.customer_id,
    c.customer_name,
    c.company_name,
    t.product_id,
    p.product_family,
    p.product_sub_family,
    t.payment_month,
    t.revenue_type,
    (t.revenue * t.quantity) AS total_revenue,
    r.country,
    r.region,
    nc.first_purchase_month,
    rm.NRR,
    rm.GRR,
    cr.lost_revenue,
    nl.fiscal_year AS new_logo_fiscal_year,
    rp.product_rank,
    rc.customer_rank
FROM {{ ref('stg_transactions') }} AS t
INNER JOIN {{ ref('stg_customers') }} AS c ON c.customer_id = t.customer_id
INNER JOIN {{ ref('stg_products') }} AS p ON p.product_id = t.product_id
INNER JOIN {{ ref('stg_regions') }} AS r ON r.customer_id = t.customer_id
LEFT JOIN new_customers nc ON nc.customer_id = t.customer_id
LEFT JOIN revenue_metrics rm ON rm.payment_month = t.payment_month
LEFT JOIN contraction_revenue cr ON cr.payment_month = t.payment_month
LEFT JOIN new_logos nl ON nl.customer_id = t.customer_id
LEFT JOIN ranked_products rp ON rp.product_id = t.product_id
LEFT JOIN ranked_customers rc ON rc.customer_id = t.customer_id
ORDER BY t.payment_month DESC
