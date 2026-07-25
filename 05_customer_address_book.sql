use role sysadmin;
use warehouse compute_wh;

use database sandbox;
use schema bronze;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

create or replace table customer_address (
    AddressID text,
    CustomerID text,
    FlatNo text,
    HouseNo text,
    Floor text,
    Building text,
    Landmark text,
    Locality text,
    City text,
    State text,
    Pincode text,
    -- Audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

create or replace stream customer_address_str
on table customer_address
append_only = true;

copy into customer_address (
    AddressID, CustomerID, FlatNo, HouseNo, Floor, Building, Landmark, Locality,
    City, State, Pincode, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as AddressID,
        t.$2::text as CustomerID,
        t.$3::text as FlatNo,
        t.$4::text as HouseNo,
        t.$5::text as Floor,
        t.$6::text as Building,
        t.$7::text as Landmark,
        t.$8::text as Locality,
        t.$9::text as City,
        t.$10::text as State,
        t.$11::text as Pincode,
        -- Audit Columns :
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @csv_stage/initial/customer_address t
)
file_format = (format_name = 'csv_file_format');

select * from customer_address;
select * from customer_address_str;

list @csv_stage/initial/customer_address;

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

create or replace table silver.customer_address (
    address_sk int autoincrement primary key,
    address_id number not null unique,
    customer_id number not null,
    flat_no string(50),
    house_no string(50),
    floor string(20),
    building string(255),
    landmark string(255),
    locality string(255),
    city string(100) not null,
    state string(100) not null,
    state_code string(2),
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    pincode string(10) not null,

    -- Audit columns :
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream silver.customer_address_stm
on table silver.customer_address;

merge into silver.customer_address as tgt
using (
    select 
        cast(AddressID as number) as address_id,
        cast(CustomerID as number) as customer_id,
        cast(FlatNo as string) as flat_no,
        cast(HouseNo as string) as house_no,
        cast(Floor as string) as floor,
        cast(Building as string) as building,
        cast(Landmark as string) as landmark,
        cast(Locality as string) as locality,
        cast(City as string) as city,
        case
            when cast(State as string) = 'Delhi' then 'New Delhi'
            else cast(State as string)
        end as state,
        case
            WHEN State = 'Delhi' THEN 'DL'
            WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP'
            WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ'
            WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB'
            WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP'
            WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH'
            WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK'
            WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS'
            WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY'
            WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP'
            WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA'
            WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG'
            WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR'
            else null
        end as state_code,
        case
            when State in ('Delhi', 'Chandigarh', 'Puducherry', 'Jammu and Kashmir') then true
            else false
        end as is_union_territory,
        case
            when (State = 'Delhi' and City = 'New Delhi') then true
            else false
        end as capital_city_flag,
        case 
            WHEN City IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad', 'New Delhi') THEN 'Tier-1'
            WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 'Coimbatore', 
                          'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        end as city_tier,
        cast(Pincode as string) as pincode,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from customer_address
) as src 
on tgt.address_id = src.address_id
when matched AND (
    src.customer_id != tgt.customer_id OR
    src.flat_no != tgt.flat_no OR
    src.house_no != tgt.house_no OR
    src.floor != tgt.floor OR
    src.building != tgt.building OR
    src.landmark != tgt.landmark OR
    src.locality != tgt.locality OR
    src.city != tgt.city OR
    src.state != tgt.state OR
    src.state_code != tgt.state_code OR
    src.is_union_territory != tgt.is_union_territory OR
    src.capital_city_flag != tgt.capital_city_flag OR
    src.city_tier != tgt.city_tier OR
    src.pincode != tgt.pincode
) then
    update set 
        tgt.customer_id = src.customer_id,
        tgt.flat_no = src.flat_no,
        tgt.house_no = src.house_no,
        tgt.floor = src.floor,
        tgt.building = src.building,
        tgt.landmark = src.landmark,
        tgt.locality = src.locality,
        tgt.city = src.city,
        tgt.state = src.state,
        tgt.state_code = src.state_code,
        tgt.is_union_territory = src.is_union_territory,
        tgt.capital_city_flag = src.capital_city_flag,
        tgt.city_tier = src.city_tier,
        tgt.pincode = src.pincode
when not matched then
    insert (
        address_id,
        customer_id,
        flat_no,
        house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        pincode,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        src.address_id,
        src.customer_id,
        src.flat_no,
        src.house_no,
        src.floor,
        src.building,
        src.landmark,
        src.locality,
        src.city,
        src.state,
        src.state_code,
        src.is_union_territory,
        src.capital_city_flag,
        src.city_tier,
        src.pincode,
        src._stg_file_name,
        src._stg_file_load_ts,
        src._stg_file_md5
    );

select * from silver.customer_address;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER (DIMENSION TABLE)
-------------------------------------------------------------------------------

create or replace table gold.customer_address_dim (
    customer_address_hk number primary key,
    address_id number not null unique,
    customer_id number not null,
    flat_no string(50),
    house_no string(50),
    floor string(20),
    building string(255),
    landmark string(255),
    locality string(255),
    city string(100) not null,
    state string(100) not null,
    state_code string(2),
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    pincode string(10) not null,
    _stg_file_name string,
    _stg_file_load_ts string,
    _stg_file_md5 string,
    eff_start_dt timestamp_tz,
    eff_end_dt timestamp_tz,
    current_flag boolean default false
);

select * from SILVER.CUSTOMER_ADDRESS_STM;

merge into gold.customer_address_dim as target
using SILVER.CUSTOMER_ADDRESS_STM as source
on 
    target.address_id = source.address_id
    and
    target.current_flag = true

when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE'
) then
    insert (
        customer_address_hk,
        address_id,
        customer_id,
        flat_no,
        house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        pincode,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_dt,
        eff_end_dt,
        current_flag 
    ) values (
        hash(SHA1_hex(concat(nvl(building,''), nvl(locality,''), city, state, pincode))),
        source.address_id,
        source.customer_id,
        source.flat_no,
        source.house_no,
        source.floor,
        source.building,
        source.landmark,
        source.locality,
        source.city,
        source.state,
        source.state_code,
        source.is_union_territory,
        source.capital_city_flag,
        source.city_tier,
        source.pincode,
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
        customer_address_hk,
        address_id,
        customer_id,
        flat_no,
        house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        pincode,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_dt,
        eff_end_dt,
        current_flag 
)
values ( 
    hash(SHA1_hex(concat(nvl(building,''), nvl(locality,''), city, state, pincode))),
    source.address_id,
    source.customer_id,
    source.flat_no,
    source.house_no,
    source.floor,
    source.building,
    source.landmark,
    source.locality,
    source.city,
    source.state,
    source.state_code,
    source.is_union_territory,
    source.capital_city_flag,
    source.city_tier,
    source.pincode,
    source._stg_file_name,
    source._stg_file_load_ts,
    source._stg_file_md5,
    current_timestamp(),
    null,
    true
);

SELECT * FROM SANDBOX.GOLD.CUSTOMER_ADDRESS_DIM;