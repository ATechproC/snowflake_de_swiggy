use role sysadmin;
use warehouse compute_wh;

use database sandbox;
use schema bronze;

create or replace table location 
(
    locationID text,
    city text,
    state text,
    zipcode text,
    activeFlag text,
    createDate text,
    modifiedDate text,
    -- Audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

create or replace stream location_str
on table location
append_only = true;

copy into location ( locationID, city, state, zipcode, activeFlag,
                      createDate, modifiedDate, _stg_file_name,
                      _stg_file_load_ts, _stg_file_md5, _copy_data_ts )
from (
    select 
        t.$1::text as locationID,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeFlag,
        t.$6::text as createDate,
        t.$7::text as modifiedDate,
        -- Audti Columns :
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @csv_stage/initial/location t
)
file_format = (format_name = 'csv_file_format');

select * from location;
select * from location_str;

-- drop table if exists location;

list @csv_stage/initial/location;

-- silver layer :

create or replace table silver.restaurant_location (
    restaurant_location_sk int autoincrement primary key,
    location_id number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz,
    modified_ts timestamp_tz,

    -- Audit columns :
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream silver.restaurant_location_stm
on table silver.restaurant_location;

-- drop table if exists silver.restaurant_location;

merge into silver.restaurant_location as tgt
using (
    select 
        cast(locationID as number) as location_id,
        cast(city as string) as city,
        case
            when cast(state as string) = 'Delhi' then 'New Delhi'
            else cast(state as string)
        end as state,
        case
            WHEN state = 'Delhi' THEN 'DL'
            WHEN state = 'Maharashtra' THEN 'MH'
            WHEN state = 'Uttar Pradesh' THEN 'UP'
            WHEN state = 'Gujarat' THEN 'GJ'
            WHEN state = 'Rajasthan' THEN 'RJ'
            WHEN state = 'Kerala' THEN 'KL'
            WHEN state = 'Punjab' THEN 'PB'
            WHEN state = 'Karnataka' THEN 'KA'
            WHEN state = 'Madhya Pradesh' THEN 'MP'
            WHEN state = 'Odisha' THEN 'OR'
            WHEN state = 'Chandigarh' THEN 'CH'
            WHEN state = 'West Bengal' THEN 'WB'
            WHEN state = 'Sikkim' THEN 'SK'
            WHEN state = 'Andhra Pradesh' THEN 'AP'
            WHEN state = 'Assam' THEN 'AS'
            WHEN state = 'Jammu and Kashmir' THEN 'JK'
            WHEN state = 'Puducherry' THEN 'PY'
            WHEN state = 'Uttarakhand' THEN 'UK'
            WHEN state = 'Himachal Pradesh' THEN 'HP'
            WHEN state = 'Tamil Nadu' THEN 'TN'
            WHEN state = 'Goa' THEN 'GA'
            WHEN state = 'Telangana' THEN 'TG'
            WHEN state = 'Chhattisgarh' THEN 'CG'
            WHEN state = 'Jharkhand' THEN 'JH'
            WHEN state = 'Bihar' THEN 'BR'
            else null
        end as state_code,
        case
            when state in ('Delhi', 'Chandigarh', 'Puducherry', 'Jammu and Kashmir') then true
            else false
        end as is_union_territory,
        case
            when (state = '' and city = '' ) then true
            when (state = '' and city = '') then true
            else false
        end as capital_city_flag,
        case 
            WHEN City IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') THEN 'Tier-1'
            WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 'Coimbatore', 
                          'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        end as city_tier,
        cast(zipcode as string) as zip_code,
        cast(activeFlag as string) as active_flag,
        to_timestamp_tz(createDate, 'YYYY-MM-DD HH24:MI:SS') as created_ts,
        to_timestamp_tz(modifiedDate, 'YYYY-MM-DD HH24:MI:SS') as modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from location
) as src 
on tgt.location_id = src.location_id
when matched AND (
    src.location_id != tgt.location_id OR
    src.city != tgt.city OR
    src.state != tgt.state OR
    src.state_code != tgt.state_code OR
    src.is_union_territory != tgt.is_union_territory OR
    src.capital_city_flag != tgt.capital_city_flag OR
    src.city_tier != tgt.city_tier OR
    src.zip_code != tgt.zip_code OR
    src.active_flag != tgt.active_flag
) then
    update set 
        tgt.location_id = src.location_id,
        tgt.city = src.city,
        tgt.state = src.state,
        tgt.state_code = src.state_code,
        tgt.is_union_territory = src.is_union_territory,
        tgt.capital_city_flag = src.capital_city_flag,
        tgt.city_tier = src.city_tier,
        tgt.zip_code = src.zip_code,
        tgt.active_flag = src.active_flag
when not matched then
    insert (
        location_id,
        city,
        state,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        zip_code,
        active_flag,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        src.location_id,
        src.city,
        src.state,
        src.state_code,
        src.is_union_territory,
        src.capital_city_flag,
        src.city_tier,
        src.zip_code,
        src.active_flag,
        src.created_ts,
        src.modified_ts,
        src._stg_file_name,
        src._stg_file_load_ts,
        src._stg_file_md5
    );

select * from silver.restaurant_location;

-- gold layer : dimension table

create or replace table gold.restaurant_location_dim (
    restaurant_location_hk number primary key,
    location_id number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz,
    modified_ts timestamp_tz,
    _stg_file_name string,
    _stg_file_load_ts string,
    _stg_file_md5 string,
    eff_start_dt timestamp_tz,
    eff_end_dt timestamp_tz,
    current_flag boolean default false
);

select * from SILVER.RESTAURANT_LOCATION_STM;

merge into gold.restaurant_location_dim as target
using SILVER.RESTAURANT_LOCATION_STM as source
on 

    target.location_id = source.location_id
    and
    target.current_flag = true

when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE'
) then
    insert (
        restaurant_location_hk,
        location_id,
        city,
        state,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        zip_code,
        active_flag,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_dt,
        eff_end_dt,
        current_flag 
    ) values (
        hash(SHA1_hex(concat(city,state,active_flag,zip_code))),
        source.location_id,
        source.city,
        source.state,
        source.state_code,
        source.is_union_territory,
        source.capital_city_flag,
        source.city_tier,
        source.zip_code,
        source.active_flag,
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
        eff_end_dt = null,
        current_flag = false
when not matched and (
    source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE'
) then insert (
        restaurant_location_hk,
        location_id,
        city,
        state,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        zip_code,
        active_flag,
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
    hash(SHA1_hex(concat(city,state,active_flag,zip_code))),
    source.location_id,
    source.city,
    source.state,
    source.state_code,
    source.is_union_territory,
    source.capital_city_flag,
    source.city_tier,
    source.zip_code,
    source.active_flag,
    source.created_ts,
    source.modified_ts,
    source._stg_file_name,
    source._stg_file_load_ts,
    source._stg_file_md5,
    current_timestamp(),
    null,
    true
);

SELECT * FROM SANDBOX.GOLD.RESTAURANT_LOCATION_DIM;