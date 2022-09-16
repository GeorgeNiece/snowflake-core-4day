-- Switch Context
USE ROLE ACCOUNTADMIN;

--Create the Warehouse
CREATE WAREHOUSE IF NOT EXISTS DATAPIPELINES_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

--- Create the database and grant access to the new role create
CREATE DATABASE IF NOT EXISTS CITIBIKE_PIPELINES;

-- Switch Context
USE CITIBIKE_PIPELINES.PUBLIC;
USE WAREHOUSE DATAPIPELINES_WH;


-- Create the table for Trips
CREATE OR REPLACE TABLE TRIPS
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);


-- Create the stage with the S3 bucket
CREATE or replace STAGE CITIBIKE_PIPELINES.PUBLIC.citibike_trips URL = 's3://snowflake-workshop-lab/citibike-trips-csv/';

list @citibike_trips;


-- Define the file format
create or replace FILE FORMAT CITIBIKE_PIPELINES.PUBLIC.CSV 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER = 0 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('');
 

alter warehouse DATAPIPELINES_WH set WAREHOUSE_SIZE = 'LARGE';
copy into trips from @citibike_trips file_format=CSV pattern = '.*.*[.]csv[.]gz';
alter warehouse DATAPIPELINES_WH set WAREHOUSE_SIZE = 'XSMALL';

-- Check we got the trips information-- Check if the trips table is loaded with data
select * from trips limit 10;
