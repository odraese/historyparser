
-- Distribution of successful/killed/failed tasks 
select status "taskFinishStatus", count(*) "eventCount" 
  from testdata.taskevents 
 group by status; 
 
-- all LLAP nodes (where hostname might be multiple times with different ports after crash) 
select substring(nodeid from 1 for position( ':' in nodeid) - 1) hostname, 
       count(distinct nodeID) - 1 "restartsAfterCrash"
  from testdata.taskevents
 where status = 'SUCCEEDED' 
 group by hostname
 order by hostname;
 
-- task duration times by task type (Map/Reducer) 
select taskType "taskType", count(*) "numTasksByType",
       cast(avg(taskdurationmillis) as numeric(10,0)) "avgDurationMS", 
       cast(stddev_pop(taskdurationmillis) as numeric(10,0)) "stdDevDurationMS" 
  from testdata.succTaskEvents
 group by "taskType";

-- get top 30 most expensive tasks
select (taskdurationmillis * 1000000) "taskDurationNano", 
       specqueuenano "specQueueNano", specrunningnano "specRunNano", 
       consumertimenano "consumerTimeNano", decodetimenano "decodeTimeNano", 
       hdfstimenano "hdfsTimeNano", totalionano "total I/O Nano", inputrecords "inputRecords", 
       inputsplitlengthbytes "inputSplitLengthBytes", decodebatches "decodeBatches", 
       vectorbatches "vectorBatches", rowsemitted "rowsEmitted", selrowgroups "selRowGroups", 
       vertexnumtasks "vertexNumTasks" 
  from testdata.succTaskEvents 
 order by taskDurationMillis desc 
 limit 30;
  
-- task duration times by node and task type (Map/Reducer) 
select hostname "hostName", tasktype "taskType", tasksintype "numTasksByType", 
       avg "avgForType", stddev "stdDevPop" 
  from testdata.averagesByNode order by "taskType", "numTasksByType" desc;

-- get the distance between the node's averages
select taskType "taskType",
       cast(avg(avg) as numeric(10,0)) "avgAllNodes",
       cast(stddev_pop(avg) as numeric(10,0)) "stdDevAllNodes",
       cast(greatest(avg(avg)-min(avg),max(avg)-avg(avg)) as numeric(10,0)) "maxDiff" ,
       cast(greatest(avg(avg)-min(avg),max(avg)-avg(avg)) * 100 / avg(avg) as numeric( 4,1 )) "maxDiffPercent" 
  from testdata.averagesByNode
  group by taskType;
  
-- skew in the cluster?  
with 
totalTimes as (
  select hostname, sum(taskdurationmillis) as totalTime, count(*) numTasks
  from testdata.succTaskEvents
  where taskType = 'Map'
  group by hostname
  order by totalTime desc
), 
maxTimeScan as (
  select max( totalTime ) maxTime from totalTimes
)
select hostname "hostName", 
       totalTime "totalTimeMS", 
       cast((totalTime * 100 / maxTime) as numeric(4,1)) "totalTimePercent", 
       numTasks "numTasks"
  from totalTimes, maxTimeScan;

  
  
  
  


  
