use role sysadmin;

create database if not exists sandbox;
use database sandbox;

create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;
create schema if not exists common;

create warehouse if not exists adhoc_wh
     comment = 'This is the adhoc-wh'
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

use schema bronze;

create or replace file format csv_file_format
    type = 'csv'
    skip_header = 1
    record_delimiter = '\n'
    field_delimiter = ','
    compression = 'auto'
    field_optionally_enclosed_by = '\042'
    null_if = ('\\N');

create or replace stage csv_stage 
    directory = (enable = true)
    comment = 'Stores incoming csv files';

-- SELECT * FROM DIRECTORY(@csv_stage);

create or replace tag 
    common.pii_policy_tag
    allowed_values 'PII', 'PRICE', 'SENSTIVE', 'EMAIL'
    comment = "This is a PII policy tag";

create or replace masking policy 
    common.email_masking_policy as (email STRING)
    returns STRING -> 
    case
        when current_role() = 'SYSADMIN'
            then email
        else '** EMAIL **'
    end;
    
create or replace masking policy
    common.phone_masking_policy as (phone STRING)
    returns STRING -> 
    case
        when current_role() = 'SYSADMIN'
            then phone
        else '** PHONE **'
    end;
    
create or replace masking policy
    common.pii_masking_policy as (pii_text STRING)
    returns STRING -> 
    case
        when current_role() = 'SYSADMIN'
            then pii_text
        else '** PII **'
    end;

LIST @csv_stage;
    