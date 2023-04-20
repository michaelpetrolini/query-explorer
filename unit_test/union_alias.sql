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

SELECT sw.id_product,
    sw.quantity,
    sw.price
FROM sw_products sw
UNION ALL
SELECT hw.id_product,
    hw.quantity,
    hw.price
FROM hw_products hw