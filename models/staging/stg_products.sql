SELECT
"product id" AS product_id,
product_family AS product_family,
"product sub family" AS product_sub_family
FROM
{{ source('raw','raw_products' )}}
WHERE
product_id IS NOT NULL