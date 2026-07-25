use role sysadmin;
use database sandbox;
use schema bronze;

use role sysadmin;

list @csv_stage/initial/customer/customers-initial.csv;

create or replace table customer (
    customerID text,
    name text,
    mobile text with tag (common.pii_policy_tag = 'PII'),
    email text with tag (common.pii_policy_tag = 'SENSTIVE'),
    login_by_using text,
    gender text with masking policy common.pii_masking_policy,
    dob text,
    anniversary text,
    preferences text,
    created_date text,
    modified_date text,

    -- audit columns :
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,

    _copy_data_ts timestamp
);

create or replace stream customer_stm 
on table customer;

copy into customer ( customerID, name, mobile , email, login_by_using, gender, 
                     dob, anniversary,preferences, created_date, modified_date, 
                     _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts )
from (
    select
        t.$1::text as customerID,
        t.$2::text as name,
        t.$3::text as mobile,
        t.$4::text as email,
        t.$5::text as login_by_using,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as prefrences,
        t.$10::text as created_date,
        t.$11::text as modified_date,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @csv_stage/initial/customer/customers-initial.csv as t
)
file_format = (format_name = 'csv_file_format');

select * from customer_stm;
select * from customer;

-- silver layer :

create or replace table silver.slv_customer (
    customer_sk number autoincrement primary key,
    customer_id number not null unique,
    name text not null,
    mobile text not null with masking policy common.phone_masking_policy,
    email text not null with masking policy common.email_masking_policy,
    login_by_using text not null,
    gender text not null with masking policy common.pii_masking_policy,
    dob text not null,
    anniversary text not null,
    preferences text not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,
    _sg_file_name text not null,
    _stg_file_load_ts text not null,
    _stg_file_md5 text not null,
    _stg_copy_ts timestamp_tz default current_timestamp()
);

create or replace stream silver.svl_customer_stm
on table silver.slv_customer;

merge into silver.slv_customer as target
using (
    select 
        cast(customerID as NUMBER) as customer_id,
        cast(name as text) as name,
        cast(mobile as text) as mobile,
        cast(email as text) as email,
        cast(login_by_using as text) as login_by_using,
        cast(gender as text) as gender,
        cast(dob as text) as dob,
        anniversary,
        preferences,
        to_timestamp_tz(created_date, 'YYYY-MM-DD') as created_ts,
        to_timestamp_tz(modified_date, 'YYYY-MM-DD') as modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    from bronze.customer
) as source
on source.customer_id = target.customer_id
when matched and (
     target.name != source.name OR
     target.mobile != source.mobile OR
     target.email != source.email OR
     target.login_by_using != source.login_by_using OR
     target.gender != source.gender OR
     target.dob != source.dob OR
     target.anniversary != source.anniversary OR
     target.preferences != source.preferences
) then
    update set
         target.name = source.name,
         target.mobile = source.mobile,
         target.email = source.email,
         target.login_by_using = source.login_by_using,
         target.gender = source.gender,
         target.dob = source.dob,
         target.anniversary = source.anniversary,
         target.preferences = source.preferences,
         target.modified_ts = source.modified_ts
when not matched then
    insert (
        customer_id,
        name,
        mobile,
        email, 
        login_by_using,
        gender,
        dob,
        anniversary,
        preferences,
        created_ts,
        modified_ts,
        _sg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        source.customer_id,
        source.name,
        source.mobile,
        source.email, 
        source.login_by_using,
        source.gender,
        source.dob,
        source.anniversary,
        source.preferences,
        source.created_ts,
        source.modified_ts,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5
    )

select * from silver.slv_customer;

-- gold layer :

create or replace table gold.customer_dim (
    customer_hk number primary key,
    customer_id number not null unique,
    name text not null,
    mobile text not null with masking policy common.phone_masking_policy,
    email text not null with masking policy common.email_masking_policy,
    login_by_using text not null,
    gender text not null with masking policy common.pii_masking_policy,
    dob text not null,
    anniversary text not null,
    preferences text not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,
    _stg_file_name text not null,
    _stg_file_load_ts text not null,
    _stg_file_md5 text not null,
    eff_start_ts timestamp,
    eff_end_ts timestamp,
    current_flag boolean default true
);

merge into
    gold.customer_dim as target
using
    silver.SVL_CUSTOMER_STM as source
on
    target.customer_id = source.customer_id
    and
    target.current_flag = true
when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE'
) THEN
    insert (
        customer_hk,
        customer_id,
        name,
        mobile,
        email,
        login_by_using,
        gender,
        dob,
        anniversary,
        preferences,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_ts,
        eff_end_ts
    ) values (
        hash(SHA1_hex(concat(name, mobile, email, login_by_using, gender, dob, anniversary, preferences))),
        source.CUSTOMER_ID,
        source.name,
        source.mobile,
        source.email,
        source.login_by_using,
        source.gender,
        source.dob,
        source.anniversary,
        source.preferences,
        source.created_ts,
        source.modified_ts,
        source._SG_FILE_NAME,
        source._stg_file_load_ts,
        source._stg_file_md5,
        current_timestamp(),
        null
    )
when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
) then
    insert (
        customer_hk,
        customer_id,
        name,
        mobile,
        email,
        login_by_using,
        gender,
        dob,
        anniversary,
        preferences,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_ts,
        eff_end_ts
    ) values (
        hash(SHA1_hex(concat(name, mobile, email, login_by_using, gender, dob, anniversary, preferences))),
        source.customer_id,
        source.name,
        source.mobile,
        source.email,
        source.login_by_using,
        source.gender,
        source.dob,
        source.anniversary,
        source.preferences,
        source.created_ts,
        source.modified_ts,
        source._SG_FILE_NAME,
        source._stg_file_load_ts,
        source._stg_file_md5,
        current_timestamp(),
        null
    )
when matched and (
    source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE'
) then
    update set
        target.eff_end_ts = current_timestamp(),
        target.current_flag = false

select * from gold.customer_dim;