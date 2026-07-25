use role sysadmin;
use warehouse compute_wh;

use database sandbox;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

create or replace table bronze.delivery (
    DeliveryID text,
    OrderID text,
    DeliveryAgentID text,
    DeliveryStatus text,
    EstimatedTime text,
    DeliveryAddress text,
    DeliveryDate text,
    CreatedDate text,
    ModifiedDate text,
    -- Audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

create or replace stream bronze.delivery_str
on table bronze.delivery;

copy into bronze.delivery (
    DeliveryID, OrderID, DeliveryAgentID, DeliveryStatus, EstimatedTime,
    DeliveryAddress, DeliveryDate, CreatedDate, ModifiedDate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as DeliveryID,
        t.$2::text as OrderID,
        t.$3::text as DeliveryAgentID,
        t.$4::text as DeliveryStatus,
        t.$5::text as EstimatedTime,
        t.$6::text as DeliveryAddress,
        t.$7::text as DeliveryDate,
        t.$8::text as CreatedDate,
        t.$9::text as ModifiedDate,
        -- Audit Columns :
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @bronze.csv_stage/initial/delivery/delivery-initial-load.csv t
)
file_format = (format_name = 'bronze.csv_file_format');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

create or replace table silver.delivery (
    delivery_sk int autoincrement primary key,
    delivery_id number not null unique,
    order_id number not null,
    delivery_agent_id number,
    delivery_status string(50) not null,
    estimated_time string(50),
    delivery_address_id number,
    delivery_ts timestamp_tz,
    created_ts timestamp_tz,
    modified_ts timestamp_tz,

    -- Audit columns :
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream silver.delivery_stm
on table silver.delivery;

merge into silver.delivery as tgt
using (
    select 
        cast(DeliveryID as number) as delivery_id,
        cast(OrderID as number) as order_id,
        cast(DeliveryAgentID as number) as delivery_agent_id,
        cast(DeliveryStatus as string) as delivery_status,
        cast(EstimatedTime as string) as estimated_time,
        cast(DeliveryAddress as number) as delivery_address_id,
        to_timestamp_tz(DeliveryDate) as delivery_ts,
        to_timestamp_tz(CreatedDate) as created_ts,
        to_timestamp_tz(ModifiedDate) as modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from bronze.delivery
) as src 
on tgt.delivery_id = src.delivery_id
when matched AND (
    src.order_id != tgt.order_id OR
    nvl(src.delivery_agent_id, -1) != nvl(tgt.delivery_agent_id, -1) OR
    src.delivery_status != tgt.delivery_status OR
    nvl(src.estimated_time, '') != nvl(tgt.estimated_time, '') OR
    nvl(src.delivery_address_id, -1) != nvl(tgt.delivery_address_id, -1) OR
    nvl(src.delivery_ts, '1970-01-01'::timestamp_tz) != nvl(tgt.delivery_ts, '1970-01-01'::timestamp_tz) OR
    src.modified_ts != tgt.modified_ts
) then
    update set 
        tgt.order_id = src.order_id,
        tgt.delivery_agent_id = src.delivery_agent_id,
        tgt.delivery_status = src.delivery_status,
        tgt.estimated_time = src.estimated_time,
        tgt.delivery_address_id = src.delivery_address_id,
        tgt.delivery_ts = src.delivery_ts,
        tgt.modified_ts = src.modified_ts
when not matched then
    insert (
        delivery_id,
        order_id,
        delivery_agent_id,
        delivery_status,
        estimated_time,
        delivery_address_id,
        delivery_ts,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        src.delivery_id,
        src.order_id,
        src.delivery_agent_id,
        src.delivery_status,
        src.estimated_time,
        src.delivery_address_id,
        src.delivery_ts,
        src.created_ts,
        src.modified_ts,
        src._stg_file_name,
        src._stg_file_load_ts,
        src._stg_file_md5
    );

select * from silver.delivery;