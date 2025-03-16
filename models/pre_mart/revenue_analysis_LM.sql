WITH current_month AS (
    SELECT 
        customer_id, 
        product_id, 
        DATE_TRUNC('month', payment_month) AS month, 
        SUM(revenue) AS current_revenue
    FROM {{ref('stg_transactions')}}
    GROUP BY 1, 2, 3
),

previous_month AS (
    SELECT 
        customer_id, 
        product_id, 
        DATE_TRUNC('month', payment_month) AS month, 
        SUM(revenue) AS previous_revenue
    FROM {{ref('stg_transactions')}}
    GROUP BY 1, 2, 3
),

revenue_comparison AS (
    SELECT 
        curr.customer_id,
        CASE 
            WHEN prev.previous_revenue IS NULL THEN curr.product_id
            ELSE prev.product_id
        END AS product_id,
        curr.month AS current_month,
        prev.month AS previous_month,
        curr.current_revenue,
        prev.previous_revenue,
        CASE 
            WHEN prev.previous_revenue IS NULL THEN 'Cross-Sell'
            WHEN curr.current_revenue IS NULL THEN 'Product Churn'
            WHEN curr.current_revenue > prev.previous_revenue THEN 'Upsell'
            WHEN curr.current_revenue < prev.previous_revenue THEN 'Downsell'
        END AS revenue_change_type,
        CASE 
            WHEN prev.previous_revenue IS NULL THEN curr.current_revenue
            WHEN curr.current_revenue IS NULL THEN prev.previous_revenue
            WHEN curr.current_revenue > prev.previous_revenue THEN curr.current_revenue-prev.previous_revenue
            WHEN curr.current_revenue < prev.previous_revenue THEN prev.previous_revenue-curr.current_revenue
        END AS revenue
    FROM current_month curr
    FULL JOIN previous_month prev
    ON curr.customer_id = prev.customer_id
    AND curr.product_id = prev.product_id
    AND prev.month = ADD_MONTHS(curr.month, -1)

)

SELECT * FROM revenue_comparison