SELECT id_order,
    quantity,
    price
FROM (
    SELECT
        id_order,
        quantity,
        price,
        partition_date
    FROM project.industry.orders
)
WHERE partition_date = "2022-12-12"