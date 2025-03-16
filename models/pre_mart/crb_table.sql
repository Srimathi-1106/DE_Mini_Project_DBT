WITH revenue AS (
    SELECT 
        product_id,
        revenue_change_type,
        SUM(revenue) AS total_revenue
    FROM 
        {{ ref('revenue_analysis_LM')  }} 
    WHERE
        revenue_change_type IS NOT NULL
    GROUP BY
        product_id,
        revenue_change_type
)

SELECT 
    product_id,
    SUM(CASE WHEN revenue_change_type = 'Downsell' THEN total_revenue ELSE 0 END) AS Down_sell,
    SUM(CASE WHEN revenue_change_type = 'Product Churn' THEN total_revenue ELSE 0 END) AS Product_churn,
    SUM(CASE WHEN revenue_change_type = 'Cross-Sell' THEN total_revenue ELSE 0 END) AS Cross_sell,
    SUM(CASE WHEN revenue_change_type = 'Upsell' THEN total_revenue ELSE 0 END) AS Up_sell
FROM
    revenue
GROUP BY
    product_id
ORDER BY
    product_id