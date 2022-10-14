-- 21.6.4 Set the context for worksheet 
USE ROLE ACCOUNTADMIN;
USE CITIBIKE_PIPELINES.PUBLIC;
USE WAREHOUSE DATAPIPELINES_WH;

-- 21.6.4 Create Cloud Storage Integration
-- the storage_aws_role_arn should be the one you copied from the mysnowflakerole that you created in AWS IAM
-- similar to arn:aws:iam::477729760828:role/mysnowflakerole

-- the storage_allowed_locations should be the bucket and prefix that you created in AWS S3 
-- similar to s3://snowflake-data-pipeline-session1-george/citibike-pipeline/

create or replace storage integration citibike_snowpipe
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = '<ARN of IAM ROLE>'
storage_allowed_locations = ('s3://<s3bucket>/<folder>/');

-- 21.6.5 Retrieve the AWS IAM User for your Snowflake Account and 
-- record the following values from the below sql statement:
-- STORAGE_AWS_IAM_USER_ARN 
-- STORAGE_AWS_EXTERNAL_ID 
-- DESC INTEGRATION <storage_integration name>;
DESC INTEGRATION citibike_snowpipe;

-- 21.7.1 Creating an External Stage. 
-- Replace the s3 bucketname and the folder name as highlighted in light green below.
create or replace stage streaming_data
  url = 's3://<s3bucket>/<folder>/'
  storage_integration = citibike_snowpipe
  file_format=(type=json);


-- 21.8.1 List stages
show stages like '%STREAMING%';


-- 22.1.1 Create a table that Snowpipe will use to write the incoming data. 
create or replace table trips_raw (v variant);

-- 22.1.2 Create a SNOW PIPE definition
-- remember you have to modify your role in AWS to have the correct external id or you will receive an error like
-- Error assuming AWS_ROLE. Please verify the role and externalId are configured correctly in your AWS policy.

-- Refer to the experiment instructions from the GitHub repo
create or replace pipe trips_pipe auto_ingest=true as copy into trips_raw from @streaming_data;

-- 22.1.3 Review the Snow pipe definition and make a note of the notification channel value.
show pipes;



-- 22.1.5 Call the Stored procedure that generates data files
call stream_data('2018-01-01', '2018-01-01');

-- 22.1.5 Verify if there are any files already in the bucket/folder
list @streaming_data;

-- 22.1.5 Check the status of the pipe. 
select system$pipe_status('trips_pipe');

-- 22.1.5 Check if the data is loaded to the trips_raw table. 
select count(*) from trips_raw;

-- 22.1.5 Review sample data from trips_raw table. 
select * from trips_raw limit 100;

-- 22.1.5 Clean up the stage by calling the purge_files function.  
call purge_files('trips_raw', '@streaming_data/');

-- 22.1.5 Truncate raw table
truncate table trips_raw;


-- 22.3.3 Check the information_schema.copy_history to see if there were any problems encountered while writing records to the trips_raw table.
select *
from table(information_schema.copy_history(
  table_name=>'citibike_pipelines.public.trips_raw',
  start_time=>dateadd(hour, -1, current_timestamp)));

-- we can query the detail for the COPY_HISTORY view in ACCOUNT_USAGE directly

select * from snowflake.account_usage.copy_history where TABLE_NAME ='TRIPS_RAW';

-- enabling other roles to use the information
select *
from table(information_schema.copy_history(table_name=>'TRIPS_RAW', start_time=> dateadd(hours, -1, current_timestamp())));
select * from snowflake.account_usage.copy_history where TABLE_NAME ='TRIPS_RAW';
use role accountadmin;

grant usage on database CITIBIKE_PIPELINES to role sysadmin;
grant usage on schema CITIBIKE_PIPELINES.information_schema to role sysadmin;
grant usage on schema PUBLIC to role sysadmin;

use role SYSADMIN;
select *
from table(information_schema.copy_history(table_name=>'TRIPS_RAW', start_time=> dateadd(hours, -1, current_timestamp())));
select * from snowflake.account_usage.copy_history where TABLE_NAME ='TRIPS_RAW';
use role accountadmin;

-- 23.1.1 Create Streams for trips and stations.
create or replace stream stream_trips on table citibike_pipelines.public.trips_raw;
create or replace stream stream_stations on table citibike_pipelines.public.trips_raw

show streams;


-- 23.1.2 Load 1 day of data to test the streams.
call stream_data('2018-01-02', '2018-01-02');

-- 23.1.3 Show the contents of the stage
list @streaming_data;
select $1 from @streaming_data limit 100;

-- 23.1.4 Check the status of the pipe and watch for file create events
select system$pipe_status('trips_pipe');

-- 23.1.5 Snowpipe copies the data into the raw table and the insertions are tracked in the stream.
select count(*) from citibike_pipelines.public.trips_raw;
select * from citibike_pipelines.public.trips_raw limit 100;

select count(*) from stream_trips;
select * from stream_trips limit 100;


-- 23.1.7 Create tables to store the data processed by the Streams.
create or replace table bike_trips (
  tripduration integer,
  starttime timestamp_ntz,
  stoptime timestamp_ntz,
  start_station_id integer,
  end_station_id integer,
  bikeid integer,
  usertype string
);

create or replace table bike_stations (
  station_id integer,
  station_name string,
  station_latitude float,
  station_longitude float,
  station_comment string
);


-- 24.1.1 Create the push_trips task to read JSON data in the streams_trips stream and load to bike_trips
create or replace task push_trips 
warehouse = DATAPIPELINES_WH
schedule = '1 minute'
when system$stream_has_data('stream_trips')
as
insert into bike_trips
  select v:tripduration::integer,
  v:starttime::timestamp_ntz,
  v:stoptime::timestamp_ntz,
  v:start_station_id::integer,
  v:end_station_id::integer,
  v:bikeid::integer,
  v:usertype::string
  from stream_trips;

-- 24.1.2 Create a task to perform a merge into the bike_stations table.
create or replace task push_stations 
warehouse = DATAPIPELINES_WH
schedule = '1 minute'
when system$stream_has_data('stream_stations')
as
merge into bike_stations s
  using (
    select v:start_station_id::integer station_id,
      v:start_station_name::string station_name,
      v:start_station_latitude::float station_latitude,
      v:start_station_longitude::float station_longitude,
      'Station at ' || v:start_station_name::string station_comment
    from stream_stations
    union
    select v:end_station_id::integer station_id,
      v:end_station_name::string station_name,
      v:end_station_latitude::float station_latitude,
      v:end_station_longitude::float station_longitude,
      'Station at ' || v:end_station_name::string station_comment
    from stream_stations) ns
  on s.station_id = ns.station_id
  when not matched then
    insert (station_id, station_name, station_latitude, station_longitude, station_comment)
    values (ns.station_id, ns.station_name, ns.station_latitude, ns.station_longitude, ns.station_comment);

-- 24.1.3 Define a TASK to call the purge_files procedure AFTER the push_trips TASK runs.
create or replace task purge_files 
warehouse = DATAPIPELINES_WH
after push_trips
as
  call purge_files('trips_raw', '@streaming_data/');

-- 24.1.4 Activate the tasks that were just created.
alter task purge_files resume;
alter task push_trips resume;
alter task push_stations resume;

show tasks;

-- 24.1.5 Verify data loaded from streams to the tables.
select count(*) from citibike_pipelines.public.bike_trips;
select count(*) from citibike_pipelines.public.bike_stations;

-- 25.1.1 Call the stream_data procedure to drop files into the STAGE for couple of days
call stream_data('2018-01-03', '2018-01-04');

-- 25.1.2 Show the details of each of the tasks over the last five minutes.
select * from table(information_schema.task_history())
  where scheduled_time > dateadd(minute, -5, current_time())
  and state <> 'SCHEDULED'
  order by completed_time desc;

-- 25.1.3 How long until the next task runs?
select timestampdiff(second, current_timestamp, scheduled_time) next_run, scheduled_time, name, state
  from table(information_schema.task_history())
  where state = 'SCHEDULED' order by completed_time desc;

-- 25.1.4 How many files have been processed by the pipeline in the last hour?
select count(*)
from table(information_schema.copy_history(
  table_name=>'citibike_pipelines.public.trips_raw',
  start_time=>dateadd(hour, -1, current_timestamp)));


-- 25.1.5 query to get an overview of the pipeline including: time to next task run, the number of files in the bucket, 
-- number of files pending for loading, total number of files processed in the last hour, and record count metrics 
-- across all the tables referenced in these labs.
select
  (select min(timestampdiff(second, current_timestamp, scheduled_time))
    from table(information_schema.task_history())
    where state = 'SCHEDULED' order by completed_time desc) time_to_next_pulse,
  (select count(distinct metadata$filename) from @streaming_data/) files_in_bucket,
  (select parse_json(system$pipe_status('citibike_pipelines.public.trips_pipe')):pendingFileCount::number) pending_file_count,
  (select count(*)
    from table(information_schema.copy_history(
    table_name=>'citibike_pipelines.public.trips_raw',
    start_time=>dateadd(hour, -1, current_timestamp)))) files_processed,
  (select count(*) from citibike_pipelines.public.trips_raw) trips_raw,
  (select count(*) from citibike_pipelines.public.stream_trips) recs_in_stream,
  (select count(*) from citibike_pipelines.public.bike_trips) bike_trips,
  (select count(*) from citibike_pipelines.public.bike_stations) bike_stations,
  (select max(starttime) from citibike_pipelines.public.trips) max_date;


-- 25.1.6 Suspend the tasks using the below queries.
alter task push_stations suspend;
alter task push_trips suspend;
alter task purge_files suspend;