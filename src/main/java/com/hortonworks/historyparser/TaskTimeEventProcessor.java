package com.hortonworks.historyparser;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.KVPair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * EventProcessor implementation to find average task execution times. This
 * implementation iterates all tasks, find their vertex initialization to
 * identify them as LLAP tasks and the task finish time to figure out the run
 * time of the task. This information can then be used to create a task
 * execution reprot with or w/o details.
 */
public class TaskTimeEventProcessor implements EventProcessor {
    private JSONParser jsonParser = null; /// < shared across all tasks
    private HashMap<String, VertexInitInfo> vertexInfoByID = null; /// < maps vertexID to VertexInitInfo instance
    private String outputFileName = null; /// < where to write thr output report
    private boolean dumpTaskDetailList = false; /// < indicator that we want to dump all task execution

    /**
     * Inner helper class to wrap a @c VERTEX_INITIALIZED event. An instance of this
     * inner class is used to wrap and store all vertex initialization events, which
     * will give us the information about task type and LLAP. This vertex
     * representation is also used as anchor for all @c TaskFinishInfo instances,
     * associated with that vertex.
     */
    private class VertexInitInfo {
        public final static String TYPE = "VERTEX_INITIALIZED"; /// < event type identifier

        private String                    vertexID      = null;   ///< the unique vertex ID
        private String                    applID        = null;   ///< belongs to that application
        private String                    vertexName    = null;   ///< name of the vertex (i.e. MAP 2)
        private String                    opType        = null;   ///< type, extracted from name
        private boolean                   isLLAP        = false;  ///< indicator that this was executed by LLAP
        private ArrayList<TaskFinishInfo> finishedTasks = null;   ///< list of tasks, assoiciated with the vertex

        /**
         * Creates a new instance, wrapping the information about a vertex.
         * 
         * @param src The history event of type @c VERTEX_INITIALIZED
         */
        public VertexInitInfo( HistoryEventProto src ) {
            int needEventDataVals = 2;

            checkNotNull( src, "source event can't be null" );
            checkArgument( TYPE.equals( src.getEventType() ) ,"Unexpected event type: %s", src.getEventType() );

            applID = src.getAppId();
            vertexID = src.getVertexId();

            // iterate all the event key/value pairs to extract information
            for ( int eventDataIdx = 0; eventDataIdx < src.getEventDataCount() && 0 < needEventDataVals; ++eventDataIdx ) {
                KVPair pair = src.getEventData( eventDataIdx );

                switch( pair.getKey() ) {
                    case "vertexName":
                        vertexName = pair.getValue();
                        --needEventDataVals;
                        break;
                    case "servicePlugin":
                        --needEventDataVals;
                        try {
                            JSONObject obj       = (JSONObject)jsonParser.parse( pair.getValue() );
                            String     scheduler = (String)obj.get( "taskSchedulerName" );

                            if ( "LLAP".equals( scheduler ) ) 
                                isLLAP = true;
                        }
                        catch( ParseException pe ) {
                            // ignore
                        }
                        break;
                }
            }
        }

        /**
         * Adds/associates a new task termination with this vertex.
         * 
         * @param tfi The event of the task termination
         */
        public void addFinishedTask( TaskFinishInfo tfi ) {
            if ( null == finishedTasks ) 
                finishedTasks = new ArrayList<>();

            finishedTasks.add( tfi );
        }

        /**
         * @return The list of all associated, task finish events.
         */
        public List<TaskFinishInfo> getFinishedTasks() {
            return finishedTasks;
        }

        /**
         * @return The applicationID of the vertex info.
         */
        public String getApplicationID() {
            return applID;
        }

        /**
         * @return The vertext identifier
         */
        public String getVertexID() {
            return vertexID;
        }

        /**
         * @return The operation (Map/Reducer), derived from the name
         */
        public String getOpType() {
            if ( null == opType ) {
                int firstBlank = vertexName.indexOf( " ", 0 );
                if ( 0 < firstBlank ) 
                    opType = vertexName.substring( 0, firstBlank );
                else    
                    opType = vertexName;
            }

            return opType;
        }

        /**
         * @return @c true of the vertex is executing on LLAP.
         */
        public boolean isLLAP() {
            return isLLAP;
        }
    }    

    /**
     * Wraps the information about a single successful task termination.
     * Instances of this class are wrapped around events of the @c TASK_FINISHED type. They give
     * access to the event specific information.
     */
    private class TaskFinishInfo {
        public final static String TYPE = "TASK_FINISHED";  ///< event type identifier

        private String  applID         = null;   ///< applicationID (from this event)
        private String  vertexID       = null;   ///< vertexID of the event
        private String  taskID         = null;   ///< task execution ID
        private long    timeTaken      = 0;      ///< task execution time
        private int     failedAttempts = 0;      ///< number of failed attempts of task
        private boolean wasSuccessful  = false;  ///< indicator that this was a successful task execution

        /**
         * Wraps a new instance of this class around a HistoryEventProto.
         */
        public TaskFinishInfo( HistoryEventProto src ) {
            int neededFields = 3;

            checkNotNull( src, "source event can't be null" );
            checkArgument( TYPE.equals( src.getEventType() ) ,"Unexpected event type: %s", src.getEventType() );

            applID = src.getAppId();
            vertexID = src.getVertexId();
            taskID = src.getTaskId();

            // iterate all event data key/value pairs until all needed fields are found
            for ( int eventDataIdx = 0; eventDataIdx < src.getEventDataCount() && 0 < neededFields; ++eventDataIdx ) {
                KVPair pair = src.getEventData( eventDataIdx );

                switch( pair.getKey() ) {
                    case "timeTaken":
                        timeTaken = Long.parseLong( pair.getValue() );
                        --neededFields;
                        break;
                    case "status":
                        wasSuccessful = "SUCCEEDED".equals( pair.getValue() );
                        --neededFields;
                        break;
                    case "numFailedTaskAttempts":
                        failedAttempts = Integer.parseInt( pair.getValue() );
                        --neededFields;
                        break;
                }
            }
        }

        /**
         * @return The application identifier of this event
         */
        public String getApplicationID() {
            return applID;
        }

        /**
         * @return The vertex identifier associated with the event.
         */
        public String getVertexID() {
            return vertexID;
        }

        /**
         * @return The unique task execution identifier
         */
        public String getTaskID() {
            return taskID;
        }

        /**
         * @return The task execution time
         */
        public long getTimeTaken() {
            return timeTaken;
        }

        /**
         * @return Total number of failed task attempts
         */
        public int getFailedAttempts() {
            return failedAttempts;
        }

        /**
         * @return Indicator that this task finished successfully.
         */
        public boolean wasSuccessful() {
            return wasSuccessful;
        }
    }

    /**
     * Creates a new event processor to count all task executions my caller and applicationID.
     * This class can create a report about all successful task executions and it also performs
     * an aggregation across the executions of a specific application identifier. 
     * 
     * @param targetFile The pathname where to store the report as text file (local)
     * @param containTaskDetailList Indicator that full task list should also be in report
     */
    public TaskTimeEventProcessor( String targetFile, boolean containTaskDetailList ) {
        checkNotNull( targetFile, "The targetFile can't be null" );
        checkArgument( 0 < targetFile.trim().length(), "The targetFile can't be an empty string" );

        vertexInfoByID = new HashMap<>();
        jsonParser = new JSONParser();
        outputFileName = targetFile;
        dumpTaskDetailList = containTaskDetailList;
    }

    @Override
    public String[] getEventFilter() {
        // this implementation only needs vertex initializations and task finish events
        return new String[] { VertexInitInfo.TYPE, TaskFinishInfo.TYPE };
    }

    @Override
    public void processNextEvent( String applID, HistoryEventProto event ) {
        checkState( applID.equals( event.getAppId() ), 
                    "Event doesn not match by ApplID (%s!=%s)", applID, event.getAppId() );

        // wrap the (successful LLAP) events into instances of the inner class
        if ( VertexInitInfo.TYPE.equals( event.getEventType() ) ) {
            VertexInitInfo vii = new VertexInitInfo( event );

            if ( vii.isLLAP() ) 
                vertexInfoByID.put( vii.getVertexID(), vii );
        }
        else if ( TaskFinishInfo.TYPE.equals( event.getEventType() ) ) {
            TaskFinishInfo tfi = new TaskFinishInfo( event );

            if ( tfi.wasSuccessful() ) {
                VertexInitInfo vii = vertexInfoByID.get( tfi.getVertexID() );

                // not having a vertexInitInfo can happen for non-LLAP tasks
                if ( null != vii ) {
                    vii.addFinishedTask( tfi );
                }
            }
        }
    }
    
    @Override
    public void allEventsReceived() {
        StringBuffer sb = new StringBuffer();

        // maps applicationID -> map[operationType]->list(taskExecutionTimes)
        HashMap<String,HashMap<String,ArrayList<Long>>> applicationTaskExecutions = new HashMap<>();

        try ( PrintWriter writer = new PrintWriter( outputFileName, "UTF-8" ) ) {
            writer.println( "Report generated: " + new Date() );

            if ( dumpTaskDetailList ) {
                writer.println();
                writer.println( "Task event data" );
                writer.println( "===================" );
    
                writer.println( "applicationID|vertexID|type|taskID|timeTaken|failedAttempts" );
            }

            // dump the detailed list of task executions
            for ( VertexInitInfo vii : vertexInfoByID.values() ) {
                // skip if we don't have any task associated with that vertex (partial file read)
                if ( null == vii.getFinishedTasks() )
                    continue;

                for ( TaskFinishInfo tfi : vii.getFinishedTasks() ) {
                    checkState( vii.getApplicationID().equals( tfi.getApplicationID() ), "Unmateched applicationID" );

                    // get the map that stores all task executions by operation type (one for each application)
                    HashMap<String,ArrayList<Long>> applicationList = applicationTaskExecutions.get( tfi.getApplicationID() );
                    if ( null == applicationList ) {
                        applicationList = new HashMap<>();
                        applicationTaskExecutions.put( tfi.getApplicationID(), applicationList );
                    }

                    // get the map from operation type (Map/Reducer) to task execution times
                    ArrayList<Long> taskTimes = applicationList.get( vii.getOpType() );
                    if ( null == taskTimes ) {
                        taskTimes = new ArrayList<>();
                        applicationList.put( vii.getOpType(), taskTimes );
                    }

                    taskTimes.add( tfi.getTimeTaken() );

                    // output the entry into the detail list
                    if ( dumpTaskDetailList ) {
                        sb.setLength( 0 );
                        sb.append( tfi.getApplicationID() );
                        sb.append( "|" );
                        sb.append( tfi.getVertexID() );
                        sb.append( "|" );
                        sb.append( vii.getOpType() );
                        sb.append( "|" );
                        sb.append( tfi.getTaskID() );
                        sb.append( "|" );
                        sb.append( Long.toString( tfi.getTimeTaken() ) );
                        sb.append( "|" );
                        sb.append( Integer.toString( tfi.getFailedAttempts() ) );
    
                        writer.println( sb );
                    }
                }
            }

            writer.println();
            writer.println( "Application aggregates" );
            writer.println( "==========================");

            writer.println( "applicationID|type|count|minTime|maxTime|avgTime|stdDevTime" );

            // now show aggregations, grouped by application identifier and type
            for ( String applicationID : applicationTaskExecutions.keySet() ) {
                HashMap<String,ArrayList<Long>> typeToValues = applicationTaskExecutions.get( applicationID );
    
                for ( String type : typeToValues.keySet() ) {
                    ArrayList<Long> taskTimes = typeToValues.get( type );
                    long minTime = Long.MAX_VALUE;
                    long maxTime = Long.MIN_VALUE;
                    long sum     = 0L;
    
                    // first pass: calculate sum, min, max, avg
                    for ( long singleTaskTime : taskTimes ) {
                        minTime = min( minTime, singleTaskTime );
                        maxTime = max( maxTime, singleTaskTime );
                        sum += singleTaskTime;
                    }
    
                    double avg = ((double)sum) / ((double)taskTimes.size());
    
                    // second pass: calculate stdDev
                    double sumDevSq = 0;
                    for ( long singleTaskTime : taskTimes ) {
                        sumDevSq += pow(((double)singleTaskTime) - avg, 2);
                    }
    
                    // population standard deviation
                    double stdDev = sqrt( sumDevSq / ((double)taskTimes.size()) );
    
                    sb.setLength( 0 );
                    sb.append( applicationID );
                    sb.append( "|" );
                    sb.append( type );
                    sb.append( "|" );
                    sb.append( Integer.toString( taskTimes.size() ) );
                    sb.append( "|" );
                    sb.append( Long.toString( minTime ) );
                    sb.append( "|" );
                    sb.append( Long.toString( maxTime ) );
                    sb.append( "|" );
                    sb.append( Long.toString( (long)avg ) );
                    sb.append( "|" );
                    sb.append( Long.toString( (long)stdDev ) );
    
                    writer.println( sb );
                }
            }
        }
        catch( IOException ioe ) {
            System.err.println( "Failed to write output file: " );
            ioe.printStackTrace();
        }
    }
}