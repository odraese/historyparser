
-- this DDL (and corresponding llap-stats.sql) can be used to LOAD and query the reports that
-- are generated via the TaskFeatureExtractor event processor. Both use PostgreSQL syntax.
drop index if exists idx_nodeID;
drop index if exists idx_taskDurationUp;
drop index if exists idx_taskDurationDown;
drop index if exists idx_taskStatus;
drop index if exists idx_hostnameType;
drop view if exists testdata.averagesByNode;
drop materialized view if exists testdata.succTaskEvents;
drop table if exists testdata.taskevents;
drop schema if exists testdata;

create schema testdata;

-- table with the single task events
create table testdata.taskevents (
    eventTime numeric not null,
    nodeID text,                      -- might be NULL for killed tasks
    contID text,                      -- might be NULL for killed tasks
    applID text not null,
    vertexID text not null,
    dagID text not null,
    taskID text not null,
    status text,
    hdfsBytesRead numeric,
    hdfsBytesWritten numeric,
    hdfsReadOps numeric,
    hdfsWriteOps numeric, 
    taskDurationMillis numeric,
    inputRecords numeric, 
    inputSplitLengthBytes numeric, 
    createdFiles numeric, 
    allocatedBytes numeric, 
    allocatedUsedBytes numeric, 
    cacheMissBytes numeric,
    consumerTimeNano numeric,
    decodeTimeNano numeric,
    hdfsTimeNano numeric,
    metaDataCacheMiss numeric,
    decodeBatches numeric,
    vectorBatches numeric,
    rowsEmitted numeric,
    selRowGroups numeric,
    totalIONano numeric,
    specQueueNano numeric,
    specRunningNano numeric, 
    vertexName text,
    vertexNumTasks numeric );

-- load report data
copy testdata.taskevents from '/absolutePathToMyReport/report.txt' (format CSV, delimiter('|'), header);

-- create statistics
analyse testdata.taskevents;

create index idx_nodeID on testdata.taskevents(nodeID);

create index idx_taskDurationUp on testdata.taskevents(taskDurationMillis asc);
create index idx_taskDurationDown on testdata.taskevents(taskDurationMillis desc);
                         
create index idx_taskStatus on testdata.taskevents(status);

-- all successful tasks
create materialized view testdata.succTaskEvents as 
  select eventTime, substring(nodeid from 1 for position( ':' in nodeid) - 1) hostname, 
         substring(vertexName from 1 for position(' ' in vertexName) - 1) taskType,  
         contID, taskID, hdfsBytesRead,
         hdfsBytesWritten, hdfsReadOps, hdfsWriteOps, taskDurationMillis, 
         inputRecords, inputSplitLengthBytes, createdFiles, allocatedBytes, 
         allocatedUsedBytes, cacheMissBytes, consumerTimeNano,
         decodeTimeNano, hdfsTimeNano, metaDataCacheMiss, decodeBatches,
         vectorBatches, rowsEmitted, selRowGroups, totalIONano,
         specQueueNano, specRunningNano, vertexName, vertexNumTasks  
    from testdata.taskevents
   where status = 'SUCCEEDED';
   
create index idx_hostnameType on testdata.succTaskEvents(hostname, taskType);
   
-- average execution times grouped by LLAP nodes
create view testdata.averagesByNode as 
   select hostname, taskType,
          count(*) as tasksInType,
          cast(avg(taskdurationmillis) as numeric(10,0)) as avg, 
          cast(stddev_pop(taskdurationmillis) as numeric(10,0)) as stddev
     from testdata.succTaskEvents
 group by hostname, taskType
 order by taskType, hostname;
   
                         