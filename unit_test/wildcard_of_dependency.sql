WITH orders AS (
    SELECT id_order,
        id_product,
        quantity,
        weekday
    FROM project.dataset.all_orders
)

SELECT *
FROM orders
WHERE weekday = '1'