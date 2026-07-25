use role sysadmin;
use warehouse compute_wh;

use database sandbox;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

create or replace table bronze.order_item (
    OrderItemID text,
    OrderID text,
    MenuID text,
    Quantity text,
    Price text,
    Subtotal text,
    CreatedDate text,
    ModifiedDate text,
    -- Audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

create or replace stream bronze.order_item_str
on table bronze.order_item;

copy into bronze.order_item (
    OrderItemID, OrderID, MenuID, Quantity, Price, Subtotal,
    CreatedDate, ModifiedDate, _stg_file_name, _stg_file_load_ts,
    _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as OrderItemID,
        t.$2::text as OrderID,
        t.$3::text as MenuID,
        t.$4::text as Quantity,
        t.$5::text as Price,
        t.$6::text as Subtotal,
        t.$7::text as CreatedDate,
        t.$8::text as ModifiedDate,
        -- Audit Columns :
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @bronze.csv_stage/initial/order-item/order-Item-initial.csv t
)
file_format = (format_name = 'bronze.csv_file_format');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

create or replace table silver.order_item (
    order_item_sk int autoincrement primary key,
    order_item_id number not null unique,
    order_id number not null,
    menu_id number not null,
    quantity number not null,
    price number(10,2) not null,
    subtotal number(10,2) not null,
    created_ts timestamp_tz,
    modified_ts timestamp_tz,

    -- Audit columns :
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream silver.order_item_stm
on table silver.order_item;

merge into silver.order_item as tgt
using (
    select 
        cast(OrderItemID as number) as order_item_id,
        cast(OrderID as number) as order_id,
        cast(MenuID as number) as menu_id,
        cast(Quantity as number) as quantity,
        cast(Price as number(10,2)) as price,
        cast(Subtotal as number(10,2)) as subtotal,
        to_timestamp_tz(CreatedDate) as created_ts,
        to_timestamp_tz(ModifiedDate) as modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from bronze.order_item
) as src 
on tgt.order_item_id = src.order_item_id
when matched AND (
    src.order_id != tgt.order_id OR
    src.menu_id != tgt.menu_id OR
    src.quantity != tgt.quantity OR
    src.price != tgt.price OR
    src.subtotal != tgt.subtotal OR
    src.modified_ts != tgt.modified_ts
) then
    update set 
        tgt.order_id = src.order_id,
        tgt.menu_id = src.menu_id,
        tgt.quantity = src.quantity,
        tgt.price = src.price,
        tgt.subtotal = src.subtotal,
        tgt.modified_ts = src.modified_ts
when not matched then
    insert (
        order_item_id,
        order_id,
        menu_id,
        quantity,
        price,
        subtotal,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        src.order_item_id,
        src.order_id,
        src.menu_id,
        src.quantity,
        src.price,
        src.subtotal,
        src.created_ts,
        src.modified_ts,
        src._stg_file_name,
        src._stg_file_load_ts,
        src._stg_file_md5
    );