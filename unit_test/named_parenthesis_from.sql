SELECT orders.id_order,
    orders.quantity,
    orders.price,
    contracts.id_contract
FROM (
    SELECT
        id_order,
        quantity,
        price,
        partition_date
    FROM project.industry.orders
) orders
JOIN project.industry.contracts contracts
ON contracts.id_order = orders.id_order
WHERE partition_date = "2022-12-12"