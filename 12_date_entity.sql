use role sysadmin;
use warehouse compute_wh;

use database sandbox;
use schema gold;

create or replace table date_dim (
    date_hk number autoincrement primary key,
    order_date DATE unique not null,
    year number not null,
    month number not null,
    quarter number not null,
    week number not null,
    day_of_year number not null,
    day_of_month number not null,
    day_of_week number not null,
    day_name string not null
);

insert into gold.date_dim
with recursive date_cte as (

    -- anchor query :
    select 
        current_date() as today,
        date(current_date()) as order_date,
        year(current_date()) as year,
        month(current_date()) as month,
        week(current_date()) as week,
        quarter(current_date()) as quarter,
        dayofyear(current_date()) as day_of_year,
        dayofmonth(current_date()) as day_of_month,
        dayofweek(current_date()) as day_of_week,
        dayname(current_date()) as dayname

    union all

    -- recursive query :

    select
        dateadd('day', -1, today) as today,
        date(dateadd('day', -1, today)) as order_date,
        year(dateadd('day', -1, today)) as year,
        month(dateadd('day', -1, today)) as month,
        week(dateadd('day', -1, today)) as week,
        quarter(dateadd('day', -1, today)) as quarter,
        dayofyear(dateadd('day', -1, today)) as day_of_year,
        dayofmonth(dateadd('day', -1, today)) as day_of_month,
        dayofweek(dateadd('day', -1, today)) as day_of_week,
        dayname(dateadd('day', -1, today)) as dayname
    from date_cte
    where order_date > (select min(date(order_ts)) from silver.orders)
)
select
    hash(order_date),
    order_date,
    year,
    month,
    week,
    quarter,
    day_of_year,
    day_of_month,
    day_of_week,
    dayname
from date_cte;