WITH sw_products AS (
    SELECT id_product,
        quantity,
        price
    FROM salesforce.software.products
),

hw_products AS (
    SELECT id_product,
        quantity,
        price
    FROM salesforce.hardware.products
)

SELECT *
FROM sw_products
UNION ALL
SELECT *
FROM hw_products