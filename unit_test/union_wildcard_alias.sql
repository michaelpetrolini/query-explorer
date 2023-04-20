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

SELECT sw_products.*
FROM sw_products
UNION ALL
SELECT hw_products.*
FROM hw_products