use role sysadmin;
use warehouse compute_wh;
use database sandbox;

create or replace table bronze.restaurant_brz (
    restaurant_id text,
    name text,
    cuisine_type text,
    pricing_for_2 text,
    phone text with masking policy common.phone_masking_policy,
    operation_hours text,
    location_id text,
    active_flag text,
    open_status text,
    address text,
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp_tz default current_timestamp()
);

list @bronze.csv_stage/initial/restaurant;

create or replace stream bronze.restaurant_brz_stm 
on table bronze.restaurant_brz;

copy into bronze.restaurant_brz (restaurant_id, name, cuisine_type, pricing_for_2, phone, operation_hours, location_id, 
            active_flag, open_status, address, _stg_file_name, _stg_file_load_ts, _stg_file_md5 )
from (
    select
        t.$1::text as restaurant_id,
        t.$2::text as name,
        t.$3::text as cuisine_type,
        t.$4::text as pricing_for_2,
        t.$5::text as phone,
        t.$6::text as operation_hours,
        t.$7::text as location_id,
        t.$8::text as active_flag,
        t.$9::text as open_status,
        t.$10::text as address,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5
    from @bronze.csv_stage/initial/restaurant/restaurant-delhi+NCR.csv as t
)
file_format = (format_name = 'bronze.csv_file_format');

-- silver layer :

create or replace table silver.restaurant_slv (
    restaurant_sk number autoincrement primary key,
    restaurant_id number unique null,
    name text not null,
    cuisine_type text not null,
    pricing_for_2 text not null,
    phone text not null,
    operation_hours text not null,
    location_id text not null,
    is_active boolean not null,
    is_open boolean not null,
    address text not null,
    _stg_file_name text not null,
    _stg_file_load_ts timestamp_tz not null,
    _stg_file_md5 text not null,
    _copy_data_ts timestamp_tz default current_timestamp()
);

create or replace stream silver.restaurant_slv_stm
on table silver.restaurant_slv;

merge into silver.restaurant_slv as target
using ( 
    select
        restaurant_id::number as restaurant_id,
        name,
        cuisine_type,
        pricing_for_2,
        phone,
        operation_hours,
        location_id::number as location_id,
        case
            when lower(active_flag) = 'yes' then TRUE
            when lower(active_flag) = 'no' then FALSE
            else null
        end as is_active,
        case 
            when lower(open_status) = 'open' then true
            WHEN LOWER(open_status) = 'close' then false
            else null
        end as is_open,
        address,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    from bronze.restaurant_brz
) as source
on target.restaurant_id = source.restaurant_id
when matched and (
    target.name != source.name OR
    target.cuisine_type != source.cuisine_type OR
    target.phone != source.phone OR
    target.operation_hours != source.operation_hours OR
    target.location_id != source.location_id OR
    target.is_active != source.is_active OR
    target.is_open != source.is_open OR
    target.address != source.address
) then
    update set 
        target.restaurant_id = source.restaurant_id,
        target.name = source.name,
        target.cuisine_type = source.cuisine_type,
        target.pricing_for_2 = source.pricing_for_2,
        target.phone = source.phone,
        target.operation_hours = source.operation_hours,
        target.is_active = source.is_active,
        target.is_open = source.is_open,
        target.address = source.address
when not matched then
    insert (
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_2,
        phone,
        operation_hours,
        location_id,
        is_active,
        is_open,
        address,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        source.restaurant_id,
        source.name,
        source.cuisine_type,
        source.pricing_for_2,
        source.phone,
        source.operation_hours,
        source.location_id,
        source.is_active,
        source.is_open,
        source.address,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5
    );

-- gold layer :

create or replace table gold.restaurant_dim (
    restaurant_hk number primary key,
    restaurant_id number unique not null,
    name text not null,
    cuisine_type text not null,
    pricing_for_2 text not null,
    phone text not null,
    operation_hours text not null,
    location_id number not null,
    is_active boolean not null,
    is_open boolean not null,
    address text not null,
    _stg_file_name text not null,
    _stg_file_load_ts timestamp_tz not null,
    _stg_file_md5 text not null,
    eff_start_ts timestamp_tz not null,
    eff_end_ts timestamp_tz default null,
    is_current boolean default true
);

merge into gold.restaurant_dim as target
using silver.restaurant_slv_stm as source
on 
    target.restaurant_id = source.restaurant_id
    and
    target.is_current = true
when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE'
) THEN
    insert (
        restaurant_hk,
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_2,
        phone,
        operation_hours,
        location_id,
        is_active,
        is_open,
        address,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_ts,
        eff_end_ts,
        is_current
    ) values (
        hash(SHA1_hex(concat(name, cuisine_type, pricing_for_2, phone, operation_hours, location_id, is_active, is_open, address))),
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_2,
        phone,
        operation_hours,
        location_id,
        is_active,
        is_open,
        address,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp(),
        null,
        true
    )
when matched and (
    source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE'
) then
    update set
        target.eff_end_ts = current_timestamp,
        target.is_current = false
when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
) THEN
    insert (
        restaurant_hk,
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_2,
        phone,
        operation_hours,
        location_id,
        is_active,
        is_open,
        address,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_ts,
        eff_end_ts,
        is_current
    ) values (
        hash(SHA1_hex(concat(name, cuisine_type, pricing_for_2, phone, operation_hours, location_id, is_active, is_open, address))),
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_2,
        phone,
        operation_hours,
        location_id,
        is_active,
        is_open,
        address,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp(),
        null,
        true
    )