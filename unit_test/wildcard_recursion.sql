WITH orders AS (
    SELECT *
    FROM project.dataset.all_orders
),

multiple_orders AS (
    SELECT *
    FROM orders
    WHERE quantity > 1
)

SELECT id_order,
    id_product,
    quantity
FROM multiple_orders
WHERE weekday = '1'