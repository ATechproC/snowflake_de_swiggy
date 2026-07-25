use role sysadmin;
use warehouse compute_wh;

use database sandbox;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

create or replace table bronze.menu (
    MenuID text,
    RestaurantID text,
    ItemName text,
    Description text,
    Price text,
    Category text,
    Availability text,
    ItemType text,
    CreatedDate text,
    ModifiedDate text,
    -- Audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

create or replace stream bronze.menu_str
on table bronze.menu;

copy into bronze.menu (
    MenuID, RestaurantID, ItemName, Description, Price, Category,
    Availability, ItemType, CreatedDate, ModifiedDate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as MenuID,
        t.$2::text as RestaurantID,
        t.$3::text as ItemName,
        t.$4::text as Description,
        t.$5::text as Price,
        t.$6::text as Category,
        t.$7::text as Availability,
        t.$8::text as ItemType,
        t.$9::text as CreatedDate,
        t.$10::text as ModifiedDate,
        -- Audit Columns :
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @bronze.csv_stage/initial/menu/menu-initial-load.csv t
)
file_format = (format_name = 'bronze.csv_file_format');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

create or replace table silver.menu (
    menu_sk int autoincrement primary key,
    menu_id number not null unique,
    restaurant_id number not null,
    item_name string(255) not null,
    description string(1000),
    price number(10,2) not null,
    category string(100) not null,
    is_available boolean not null default true,
    item_type string(50) not null,
    created_ts timestamp_tz,
    modified_ts timestamp_tz,

    -- Audit columns :
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream silver.menu_stm
on table silver.menu;

merge into silver.menu as tgt
using (
    select 
        cast(MenuID as number) as menu_id,
        cast(RestaurantID as number) as restaurant_id,
        cast(ItemName as string) as item_name,
        cast(Description as string) as description,
        cast(Price as number(10,2)) as price,
        cast(Category as string) as category,
        cast(Availability as boolean) as is_available,
        cast(ItemType as string) as item_type,
        to_timestamp_tz(CreatedDate) as created_ts,
        to_timestamp_tz(ModifiedDate) as modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from bronze.menu
) as src 
on tgt.menu_id = src.menu_id
when matched AND (
    src.restaurant_id != tgt.restaurant_id OR
    src.item_name != tgt.item_name OR
    src.description != tgt.description OR
    src.price != tgt.price OR
    src.category != tgt.category OR
    src.is_available != tgt.is_available OR
    src.item_type != tgt.item_type OR
    src.modified_ts != tgt.modified_ts
) then
    update set 
        tgt.restaurant_id = src.restaurant_id,
        tgt.item_name = src.item_name,
        tgt.description = src.description,
        tgt.price = src.price,
        tgt.category = src.category,
        tgt.is_available = src.is_available,
        tgt.item_type = src.item_type,
        tgt.modified_ts = src.modified_ts
when not matched then
    insert (
        menu_id,
        restaurant_id,
        item_name,
        description,
        price,
        category,
        is_available,
        item_type,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        src.menu_id,
        src.restaurant_id,
        src.item_name,
        src.description,
        src.price,
        src.category,
        src.is_available,
        src.item_type,
        src.created_ts,
        src.modified_ts,
        src._stg_file_name,
        src._stg_file_load_ts,
        src._stg_file_md5
    );

-------------------------------------------------------------------------------
-- 3. GOLD LAYER (DIMENSION TABLE - SCD TYPE 2)
-------------------------------------------------------------------------------

create or replace table gold.menu_dim (
    menu_hk number primary key,
    menu_id number not null,
    restaurant_id number not null,
    item_name string(255) not null,
    description string(1000),
    price number(10,2) not null,
    category string(100) not null,
    is_available boolean not null default true,
    item_type string(50) not null,
    created_ts timestamp_tz,
    modified_ts timestamp_tz,
    _stg_file_name string,
    _stg_file_load_ts string,
    _stg_file_md5 string,
    eff_start_dt timestamp_tz,
    eff_end_dt timestamp_tz,
    current_flag boolean default false
);

merge into gold.menu_dim as target
using SILVER.MENU_STM as source
on 
    target.menu_id = source.menu_id
    and
    target.current_flag = true

when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE'
) then
    insert (
        menu_hk,
        menu_id,
        restaurant_id,
        item_name,
        description,
        price,
        category,
        is_available,
        item_type,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_dt,
        eff_end_dt,
        current_flag 
    ) values (
        hash(SHA1_hex(concat(item_name, to_varchar(price), category, to_varchar(is_available), item_type))),
        source.menu_id,
        source.restaurant_id,
        source.item_name,
        source.description,
        source.price,
        source.category,
        source.is_available,
        source.item_type,
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
        menu_hk,
        menu_id,
        restaurant_id,
        item_name,
        description,
        price,
        category,
        is_available,
        item_type,
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
    hash(SHA1_hex(concat(item_name, to_varchar(price), category, to_varchar(is_available), item_type))),
    source.menu_id,
    source.restaurant_id,
    source.item_name,
    source.description,
    source.price,
    source.category,
    source.is_available,
    source.item_type,
    source.created_ts,
    source.modified_ts,
    source._stg_file_name,
    source._stg_file_load_ts,
    source._stg_file_md5,
    current_timestamp(),
    null,
    true
);

select * from gold.menu_dim;