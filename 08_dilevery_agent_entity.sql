use role sysadmin;
use warehouse compute_wh;

use database sandbox;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

create or replace table bronze.delivery_agent (
    DeliveryAgentID text,
    Name text,
    Phone text,
    VehicleType text,
    LocationID text,
    Status text,
    Gender text,
    Rating text,
    CreatedDate text,
    ModifiedDate text,
    -- Audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

create or replace stream bronze.delivery_agent_str
on table bronze.delivery_agent;

copy into bronze.delivery_agent (
    DeliveryAgentID, Name, Phone, VehicleType, LocationID, Status,
    Gender, Rating, CreatedDate, ModifiedDate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as DeliveryAgentID,
        t.$2::text as Name,
        t.$3::text as Phone,
        t.$4::text as VehicleType,
        t.$5::text as LocationID,
        t.$6::text as Status,
        t.$7::text as Gender,
        t.$8::text as Rating,
        t.$9::text as CreatedDate,
        t.$10::text as ModifiedDate,
        -- Audit Columns :
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @bronze.csv_stage/initial/delivery-agent/delivery-agent-initial.csv t
)
file_format = (format_name = 'bronze.csv_file_format');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

create or replace table silver.delivery_agent (
    delivery_agent_sk int autoincrement primary key,
    delivery_agent_id number not null unique,
    name string(255) not null,
    phone string(20),
    vehicle_type string(50),
    location_id number,
    status string(50) not null,
    gender string(20),
    rating number(3,2),
    created_ts timestamp_tz,
    modified_ts timestamp_tz,

    -- Audit columns :
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream silver.delivery_agent_stm
on table silver.delivery_agent;

merge into silver.delivery_agent as tgt
using (
    select 
        cast(DeliveryAgentID as number) as delivery_agent_id,
        cast(Name as string) as name,
        cast(Phone as string) as phone,
        cast(VehicleType as string) as vehicle_type,
        cast(LocationID as number) as location_id,
        cast(Status as string) as status,
        cast(Gender as string) as gender,
        cast(Rating as number(3,2)) as rating,
        to_timestamp_tz(CreatedDate) as created_ts,
        to_timestamp_tz(ModifiedDate) as modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from bronze.delivery_agent
) as src 
on tgt.delivery_agent_id = src.delivery_agent_id
when matched AND (
    src.name != tgt.name OR
    nvl(src.phone, '') != nvl(tgt.phone, '') OR
    nvl(src.vehicle_type, '') != nvl(tgt.vehicle_type, '') OR
    nvl(src.location_id, -1) != nvl(tgt.location_id, -1) OR
    src.status != tgt.status OR
    nvl(src.gender, '') != nvl(tgt.gender, '') OR
    nvl(src.rating, -1) != nvl(tgt.rating, -1) OR
    src.modified_ts != tgt.modified_ts
) then
    update set 
        tgt.name = src.name,
        tgt.phone = src.phone,
        tgt.vehicle_type = src.vehicle_type,
        tgt.location_id = src.location_id,
        tgt.status = src.status,
        tgt.gender = src.gender,
        tgt.rating = src.rating,
        tgt.modified_ts = src.modified_ts
when not matched then
    insert (
        delivery_agent_id,
        name,
        phone,
        vehicle_type,
        location_id,
        status,
        gender,
        rating,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        src.delivery_agent_id,
        src.name,
        src.phone,
        src.vehicle_type,
        src.location_id,
        src.status,
        src.gender,
        src.rating,
        src.created_ts,
        src.modified_ts,
        src._stg_file_name,
        src._stg_file_load_ts,
        src._stg_file_md5
    );

-------------------------------------------------------------------------------
-- 3. GOLD LAYER (DIMENSION TABLE - SCD TYPE 2)
-------------------------------------------------------------------------------

create or replace table gold.delivery_agent_dim (
    delivery_agent_hk number primary key,
    delivery_agent_id number not null,
    name string(255) not null,
    phone string(20),
    vehicle_type string(50),
    location_id number,
    status string(50) not null,
    gender string(20),
    rating number(3,2),
    created_ts timestamp_tz,
    modified_ts timestamp_tz,
    _stg_file_name string,
    _stg_file_load_ts string,
    _stg_file_md5 string,
    eff_start_dt timestamp_tz,
    eff_end_dt timestamp_tz,
    current_flag boolean default false
);

merge into gold.delivery_agent_dim as target
using SILVER.DELIVERY_AGENT_STM as source
on 
    target.delivery_agent_id = source.delivery_agent_id
    and
    target.current_flag = true

when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE'
) then
    insert (
        delivery_agent_hk,
        delivery_agent_id,
        name,
        phone,
        vehicle_type,
        location_id,
        status,
        gender,
        rating,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_dt,
        eff_end_dt,
        current_flag 
    ) values (
        hash(SHA1_hex(concat(name, nvl(phone,''), nvl(vehicle_type,''), status, nvl(to_varchar(rating),'')))),
        source.delivery_agent_id,
        source.name,
        source.phone,
        source.vehicle_type,
        source.location_id,
        source.status,
        source.gender,
        source.rating,
        source.created_ts,
        source.modified_ts,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        current_timestamp(),
        null,
        true
    )
when matched and (
    source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE'
) then 
    update set
        eff_end_dt = current_timestamp(),
        current_flag = false
when not matched and (
    source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE'
) then insert (
        delivery_agent_hk,
        delivery_agent_id,
        name,
        phone,
        vehicle_type,
        location_id,
        status,
        gender,
        rating,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_dt,
        eff_end_dt,
        current_flag 
)
values ( 
    hash(SHA1_hex(concat(name, nvl(phone,''), nvl(vehicle_type,''), status, nvl(to_varchar(rating),'')))),
    source.delivery_agent_id,
    source.name,
    source.phone,
    source.vehicle_type,
    source.location_id,
    source.status,
    source.gender,
    source.rating,
    source.created_ts,
    source.modified_ts,
    source._stg_file_name,
    source._stg_file_load_ts,
    source._stg_file_md5,
    current_timestamp(),
    null,
    true
);

select * from gold.delivery_agent_dim;