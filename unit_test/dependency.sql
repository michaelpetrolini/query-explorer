with customers as (
	select id,
		name,
		surname
	from project.tables.customers
	where partition_date= "2022-12-12"
),

paychecks as (
	select id,
		paycheck
	from project.tables.paychecks
	where pay_month="12"
)

select name as c_name,
	c.surname as c_surname,
	p.paycheck
from customers c
join paychecks p
on c.id = p.id