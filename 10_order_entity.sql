use role sysadmin;
use warehouse compute_wh;

use database sandbox;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

create or replace table bronze.orders (
    OrderID text,
    CustomerID text,
    RestaurantID text,
    OrderDate text,
    TotalAmount text,
    Status text,
    PaymentMethod text,
    CreatedDate text,
    ModifiedDate text,
    -- Audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

create or replace stream bronze.orders_str
on table bronze.orders;

copy into bronze.orders (
    OrderID, CustomerID, RestaurantID, OrderDate, TotalAmount,
    Status, PaymentMethod, CreatedDate, ModifiedDate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as OrderID,
        t.$2::text as CustomerID,
        t.$3::text as RestaurantID,
        t.$4::text as OrderDate,
        t.$5::text as TotalAmount,
        t.$6::text as Status,
        t.$7::text as PaymentMethod,
        t.$8::text as CreatedDate,
        t.$9::text as ModifiedDate,
        -- Audit Columns :
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @bronze.csv_stage/initial/orders/orders-initial.csv t
)
file_format = (format_name = 'bronze.csv_file_format');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

create or replace table silver.orders (
    order_sk int autoincrement primary key,
    order_id number not null unique,
    customer_id number not null,
    restaurant_id number not null,
    order_ts timestamp_tz not null,
    total_amount number(10,2) not null,
    status string(50) not null,
    payment_method string(50),
    created_ts timestamp_tz,
    modified_ts timestamp_tz,

    -- Audit columns :
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream silver.orders_stm
on table silver.orders;

merge into silver.orders as tgt
using (
    select 
        cast(OrderID as number) as order_id,
        cast(CustomerID as number) as customer_id,
        cast(RestaurantID as number) as restaurant_id,
        to_timestamp_tz(OrderDate) as order_ts,
        cast(TotalAmount as number(10,2)) as total_amount,
        cast(Status as string) as status,
        cast(PaymentMethod as string) as payment_method,
        to_timestamp_tz(CreatedDate) as created_ts,
        to_timestamp_tz(ModifiedDate) as modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from bronze.orders
) as src 
on tgt.order_id = src.order_id
when matched AND (
    src.customer_id != tgt.customer_id OR
    src.restaurant_id != tgt.restaurant_id OR
    src.order_ts != tgt.order_ts OR
    src.total_amount != tgt.total_amount OR
    src.status != tgt.status OR
    nvl(src.payment_method, '') != nvl(tgt.payment_method, '') OR
    src.modified_ts != tgt.modified_ts
) then
    update set 
        tgt.customer_id = src.customer_id,
        tgt.restaurant_id = src.restaurant_id,
        tgt.order_ts = src.order_ts,
        tgt.total_amount = src.total_amount,
        tgt.status = src.status,
        tgt.payment_method = src.payment_method,
        tgt.modified_ts = src.modified_ts
when not matched then
    insert (
        order_id,
        customer_id,
        restaurant_id,
        order_ts,
        total_amount,
        status,
        payment_method,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        src.order_id,
        src.customer_id,
        src.restaurant_id,
        src.order_ts,
        src.total_amount,
        src.status,
        src.payment_method,
        src.created_ts,
        src.modified_ts,
        src._stg_file_name,
        src._stg_file_load_ts,
        src._stg_file_md5
    );