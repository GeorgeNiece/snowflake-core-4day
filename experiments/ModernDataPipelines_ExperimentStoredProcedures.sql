-- Set context
USE ROLE ACCOUNTADMIN;
USE CITIBIKE_PIPELINES.PUBLIC;


-- In order to mimic periodically arriving data, this stored procedure trickle-unloads data from the TRIPS table in the CITIBIKE_PIPELINES DB into JSON files in the stage. 
-- The procedure takes a start and stop date range and aggregates records on a daily basis and then writes the files out to the bucket location in 5 second intervals.
create or replace procedure stream_data (START_DATE STRING, STOP_DATE STRING)
returns float
language javascript strict
as
$$
var counter = 0;

// list the partition values
var days = snowflake.execute({ sqlText: `
    select 
        distinct to_char(date(starttime))
    from citibike_pipelines.public.trips
    where 
        to_date(starttime) >= to_date('` + START_DATE + `')
        and to_date(starttime) <= to_date('` + STOP_DATE + `')
    order by 1;` });


// for each partition
while (days.next())
{
    var day = days.getColumnValue(1);
    var unload_qry = snowflake.execute({ sqlText: `
    copy into @streaming_data/` + day + ` from (
    select object_construct(
        'tripduration', tripduration,
        'starttime', starttime,
        'stoptime', stoptime,
        'start_station_id', start_station_id,
        'start_station_name', start_station_name,
        'start_station_latitude', start_station_latitude,
        'start_station_longitude', start_station_longitude,
        'end_station_id', end_station_id,
        'end_station_name', end_station_name,
        'end_station_latitude', end_station_latitude,
        'end_station_longitude', end_station_longitude,
        'bikeid', bikeid,
        'usertype', usertype)
        from citibike_pipelines.public.trips 
        where to_date(starttime) = to_date('` + day + `')
    order by starttime);` });

    counter++;

    // sleep for five seconds
    var wake = new Date();
    var now = new Date();
    wake = Date.now() + 10000;
    do { now = Date.now(); }
      while (now <= wake);
}

return counter;
$$;

-- The procedure here will be used to clean up files that are successfully loaded. 
-- It will compare records entries in the information_schema.copy_history view with the file names still present in the stage. 
-- Files that were found to have been loaded are now safely deleted by the procedure.
create or replace procedure purge_files (TABLE_NAME STRING, STAGE_NAME STRING)
  returns float
  language javascript strict
  execute as caller
as
$$
  var counter = 0;
  var sqlRemove = "";
  var sqlFiles = "select listagg('.*' || h.file_name, '|'), count(distinct h.file_name)" +
                 "  from table(information_schema.copy_history(" +
                 "    table_name=>'" + TABLE_NAME + "'," +
                 "    start_time=>dateadd(hour, -10, current_timestamp))) h" +
                 "  inner join (select distinct metadata$filename filename from " + STAGE_NAME  + ") f" +
                 "    on f.filename = (h.stage_location || h.file_name)" +
                 "  where h.error_count = 0;"
  // list the files to purge
  var files = snowflake.execute({ sqlText: sqlFiles });
  // for each file
  while (files.next())
  {
    var file = files.getColumnValue(1);
    sqlRemove = "rm " + STAGE_NAME + " pattern='" + file + "';";
    try {
        var unload_qry = snowflake.execute({ sqlText: sqlRemove });
        counter = files.getColumnValue(2);
    }
    catch (err) {
        counter = 0;
    }
}
  return counter;
$$;

-- list the stored procedures
show procedures;