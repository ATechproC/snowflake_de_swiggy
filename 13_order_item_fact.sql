use role sysadmin;
use warehouse compute_wh;
use database sandbox;
use schema gold;

create or replace table gold.order_item_fact (
    order_item_sk number autoincrement primary key,
    order_id number not null,
    order_item_id number not null,
    restaurant_location_dim_key number not null,
    restaurant_dim_key number not null,
    customer_dim_key number not null,
    customer_address_book_dim_key number not null,
    menu_dim_key number not null,
    login_audit_dim_key number not null,
    dilevery_agent_dim_key number not null,
    delivery_dim_key number,
    date_dim_key number not null,
    quantity number not null,
    price number not null,
    subtotal number not null,
    delivery_status text,
    estimated_time text,
    _copy_data_ts timestamp_tz default current_timestamp()
);

merge into gold.order_item_fact as target 
using (
    select
        o.order_id,
        oi.order_item_id,
        rl.restaurant_location_hk,
        r.restaurant_hk,
        c.customer_hk,
        ca.customer_address_hk,
        m.menu_hk,
        la.login_audit_hk,
        da.delivery_agent_hk,
        d.delivery_sk,
        dt.date_hk,
        oi.price,
        oi.subtotal,
        oi.quantity,
        d.delivery_status,
        d.estimated_time
    from silver.order_item_stm as oi
    left join silver.orders as o
    on  o.order_id = oi.order_id
    left join gold.restaurant_dim as r
    on o.restaurant_id = r.restaurant_id
    left join gold.restaurant_location_dim as rl
    on r.location_id = rl.location_id
    left join gold.customer_dim as c
    on o.customer_id = c.customer_id
    left join gold.customer_address_dim as ca
    on c.customer_id = ca.customer_id
    left join gold.menu_dim as m
    on oi.menu_id = m.menu_id
    left join gold.login_audit_dim as la
    on la.customer_id = c.customer_id
    left join gold.delivery_agent_dim as da
    on da.location_id = rl.location_id
    left join silver.delivery as d
    on da.delivery_agent_id = d.delivery_agent_id
    left join gold.date_dim as dt
    on dt.order_date = date(order_ts)
) as source
on 
    source.order_item_id = target.order_item_id
    and
    source.order_id = target.order_id
when not matched then
    insert (
        order_id,
        order_item_id,
        restaurant_location_dim_key,
        restaurant_dim_key,
        customer_dim_key,
        customer_address_book_dim_key,
        menu_dim_key,
        login_audit_dim_key,
        dilevery_agent_dim_key,
        delivery_dim_key,
        date_dim_key,
        quantity,
        price,
        subtotal,
        delivery_status,
        estimated_time
    ) values (
        source.order_id,
        source.order_item_id,
        source.restaurant_location_hk,
        source.restaurant_hk,
        source.customer_hk,
        source.customer_address_hk,
        source.menu_hk,
        source.login_audit_hk,
        source.DELIVERY_AGENT_HK,
        source.DELIVERY_SK,
        source.date_hk,
        source.quantity,
        source.price,
        source.subtotal,
        source.delivery_status,
        source.estimated_time
    )
when matched then
    update set
        target.restaurant_location_dim_key = source.restaurant_location_hk,
        target.customer_dim_key = source.customer_hk,
        target.customer_address_book_dim_key = source.customer_address_hk,
        target.menu_dim_key = source.menu_hk,
        target.login_audit_dim_key = source.login_audit_hk,
        target.dilevery_agent_dim_key = source.DELIVERY_AGENT_HK,
        target.delivery_dim_key = source.DELIVERY_SK,
        target.date_dim_key = source.date_hk,
        target.quantity = source.quantity,
        target.price = source.price,
        target.subtotal = source.subtotal,
        target.delivery_status = source.delivery_status,
        target.estimated_time = source.estimated_time,
        target.restaurant_dim_key = source.restaurant_hk;

-- Restaurant Location Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_restaurant_location
FOREIGN KEY (restaurant_location_dim_key)
REFERENCES gold.restaurant_location_dim (restaurant_location_hk);

-- Restaurant Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_restaurant
FOREIGN KEY (restaurant_dim_key)
REFERENCES gold.restaurant_dim (restaurant_hk);

-- Customer Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_customer
FOREIGN KEY (customer_dim_key)
REFERENCES gold.customer_dim (customer_hk);

-- Customer Address Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_customer_address
FOREIGN KEY (customer_address_book_dim_key)
REFERENCES gold.customer_address_dim (customer_address_hk);

-- Menu Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_menu
FOREIGN KEY (menu_dim_key)
REFERENCES gold.menu_dim (menu_hk);

-- Login Audit Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_login_audit
FOREIGN KEY (login_audit_dim_key)
REFERENCES gold.login_audit_dim (login_audit_hk);

-- Delivery Agent Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_delivery_agent
FOREIGN KEY (dilevery_agent_dim_key)
REFERENCES gold.delivery_agent_dim (delivery_agent_hk);

-- Delivery Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_delivery
FOREIGN KEY (delivery_dim_key)
REFERENCES  SANDBOX.SILVER.DELIVERY (delivery_sk);

-- Date Dimension
ALTER TABLE gold.order_item_fact
ADD CONSTRAINT fk_order_item_date
FOREIGN KEY (date_dim_key)
REFERENCES gold.date_dim (date_hk);

select * from gold.order_item_fact;