-- Switch Context
USE ROLE ACCOUNTADMIN;
USE CITIBIKE_PIPELINES.PUBLIC;
USE WAREHOUSE DATAPIPELINES_WH;

call purge_files('trips_raw', '@streaming_data/');
drop database if exists CITIBIKE_PIPELINES;
drop warehouse if exists DATAPIPELINES_WH;
