WITH orders AS (
    SELECT *
    FROM project.dataset.all_orders
)

SELECT id_order,
    id_product,
    quantity
FROM orders
WHERE weekday = '1'