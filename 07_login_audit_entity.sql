use role sysadmin;
use warehouse compute_wh;

use database sandbox;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

create or replace table bronze.login_audit (
    AuditID text,
    CustomerID text,
    LoginTimestamp text,
    IPAddress text,
    DeviceType text,
    LoginStatus text,
    FailureReason text,
    -- Audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

create or replace stream bronze.login_audit_str
on table bronze.login_audit;

copy into bronze.login_audit (
    AuditID, CustomerID, LoginTimestamp, IPAddress, DeviceType,
    LoginStatus, FailureReason, _stg_file_name, _stg_file_load_ts,
    _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as AuditID,
        t.$2::text as CustomerID,
        t.$3::text as LoginTimestamp,
        t.$4::text as IPAddress,
        t.$5::text as DeviceType,
        t.$6::text as LoginStatus,
        t.$7::text as FailureReason,
        -- Audit Columns :
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from @bronze.csv_stage/initial/login-audit/login-audit-initial.csv t
)
file_format = (format_name = 'bronze.csv_file_format');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

create or replace table silver.login_audit (
    login_audit_sk int autoincrement primary key,
    audit_id number not null unique,
    customer_id number not null,
    login_ts timestamp_tz not null,
    ip_address string(45),
    device_type string(100),
    login_status string(50) not null,
    failure_reason string(255),

    -- Audit columns :
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
);

create or replace stream silver.login_audit_stm
on table silver.login_audit;

merge into silver.login_audit as tgt
using (
    select 
        cast(AuditID as number) as audit_id,
        cast(CustomerID as number) as customer_id,
        to_timestamp_tz(LoginTimestamp) as login_ts,
        cast(IPAddress as string) as ip_address,
        cast(DeviceType as string) as device_type,
        cast(LoginStatus as string) as login_status,
        cast(FailureReason as string) as failure_reason,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        current_timestamp() as _copy_data_ts
    from bronze.login_audit
) as src 
on tgt.audit_id = src.audit_id
when matched AND (
    src.customer_id != tgt.customer_id OR
    src.login_ts != tgt.login_ts OR
    src.ip_address != tgt.ip_address OR
    src.device_type != tgt.device_type OR
    src.login_status != tgt.login_status OR
    nvl(src.failure_reason, '') != nvl(tgt.failure_reason, '')
) then
    update set 
        tgt.customer_id = src.customer_id,
        tgt.login_ts = src.login_ts,
        tgt.ip_address = src.ip_address,
        tgt.device_type = src.device_type,
        tgt.login_status = src.login_status,
        tgt.failure_reason = src.failure_reason
when not matched then
    insert (
        audit_id,
        customer_id,
        login_ts,
        ip_address,
        device_type,
        login_status,
        failure_reason,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    ) values (
        src.audit_id,
        src.customer_id,
        src.login_ts,
        src.ip_address,
        src.device_type,
        src.login_status,
        src.failure_reason,
        src._stg_file_name,
        src._stg_file_load_ts,
        src._stg_file_md5
    );

-------------------------------------------------------------------------------
-- 3. GOLD LAYER (DIMENSION TABLE - SCD TYPE 2)
-------------------------------------------------------------------------------

create or replace table gold.login_audit_dim (
    login_audit_hk number primary key,
    audit_id number not null,
    customer_id number not null,
    login_ts timestamp_tz not null,
    ip_address string(45),
    device_type string(100),
    login_status string(50) not null,
    failure_reason string(255),
    _stg_file_name string,
    _stg_file_load_ts string,
    _stg_file_md5 string,
    eff_start_dt timestamp_tz,
    eff_end_dt timestamp_tz,
    current_flag boolean default false
);

merge into gold.login_audit_dim as target
using SILVER.LOGIN_AUDIT_STM as source
on 
    target.audit_id = source.audit_id
    and
    target.current_flag = true

when not matched and (
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE'
) then
    insert (
        login_audit_hk,
        audit_id,
        customer_id,
        login_ts,
        ip_address,
        device_type,
        login_status,
        failure_reason,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_dt,
        eff_end_dt,
        current_flag 
    ) values (
        hash(SHA1_hex(concat(to_varchar(customer_id), nvl(ip_address,''), nvl(device_type,''), login_status, nvl(failure_reason,'')))),
        source.audit_id,
        source.customer_id,
        source.login_ts,
        source.ip_address,
        source.device_type,
        source.login_status,
        source.failure_reason,
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
        login_audit_hk,
        audit_id,
        customer_id,
        login_ts,
        ip_address,
        device_type,
        login_status,
        failure_reason,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        eff_start_dt,
        eff_end_dt,
        current_flag 
)
values ( 
    hash(SHA1_hex(concat(to_varchar(customer_id), nvl(ip_address,''), nvl(device_type,''), login_status, nvl(failure_reason,'')))),
    source.audit_id,
    source.customer_id,
    source.login_ts,
    source.ip_address,
    source.device_type,
    source.login_status,
    source.failure_reason,
    source._stg_file_name,
    source._stg_file_load_ts,
    source._stg_file_md5,
    current_timestamp(),
    null,
    true
);

select * from gold.login_audit_dim;