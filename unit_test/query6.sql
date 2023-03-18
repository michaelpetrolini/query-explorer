with customers as (
	select id,
		name,
		surname,
		office_id
	from project.tables.customers
	where partition_date= "2022-12-12"
),

paychecks as (
	select id,
		paycheck
	from project.tables.paychecks
	where pay_month="12"
),

costs as (
	select office_id,
		p.paycheck
	from customers c
	join paychecks p
	on c.id = p.id
),

managers as (
	select manager_id,
		id_office
	from project.tables.managers
)

select manager_id,
	paycheck as cost
from managers m
join costs c
on m.id_office=c.office_id

