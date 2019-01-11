package com.cloudera.historyparser.processors;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import com.cloudera.historyparser.EventProcessor;

import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.KVPair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @c EventProcessor implementation to externalize all task attempt finish event data.
 * This implementation is a simple converter from the daga_data protobuf content into a text
 * file, which can be used as input for other processing. It exports the content as '|' 
 * separated list, adding also information from the corresponding Vertex Info event and 
 * filtering the events for LLAP events only.
 */
public class TaskFeatureExtractor implements EventProcessor {
    private final static String VERTEX_INIT        = "VERTEX_INITIALIZED";    ///< event type identifier
    private final static String TASK_ATMT_START    = "TASK_ATTEMPT_STARTED";  ///< beginning of a task
    private final static String TASK_ATMT_FINISHED = "TASK_ATTEMPT_FINISHED"; ///< event type identifier

    private final static Logger LOG = LoggerFactory.getLogger( TaskFeatureExtractor.class );

    /// Data keeper POJO for the data of a TASK_ATTEMPT_START event
    private static class AttemptData {
        public AttemptData( String nodeID, String contID ) {
            this.nodeID = nodeID;
            this.containerID = contID;
        }

        String nodeID;         ///< the host/node ID
        String containerID;    ///< unique containerID
    }

    private JSONParser                        jsonParser              = null;  ///< shared across all tasks
    private HashMap<String,HistoryEventProto> vertexIDToVertexInit    = null;  ///< maps vertexID to vertex init event
    private HashMap<String,AttemptData>       taskAtmtIDToStart       = null;  ///< stores the task start data
    private File                              targetFile              = null;  ///< report file to write to
    private String[]                          lineBuffer              = null;  ///< holds multiple lines before writing to report
    private int                               lineBufferIdx           = 0;     ///< next free slot in lineBuffer
    private boolean                           hadPreviousLineBuffers  = false; ///< do we need to overwire or append to report
    private boolean                           headerWritten           = false; ///< indicator if the header line was already created 
    private StringBuffer                      sb                      = null;  ///< buffer to create report line output

    /**
     * Creates a new feature extractor, writint to the specified report file.
     * The so created instance will consime all @c HistoryEventProto instances and convert
     * each of them into a line in the report file.
     * 
     * @param reportFile name of the file to write to
     */
    public TaskFeatureExtractor( String reportFile ) {
        jsonParser = new JSONParser();
        vertexIDToVertexInit = new HashMap<>();
        taskAtmtIDToStart = new HashMap<>();
        sb = new StringBuffer();

        // write 2K lines at a time
        lineBuffer = new String[2048];

        targetFile = new File( reportFile );
        if ( targetFile.exists() && targetFile.isDirectory() ) 
            throw new IllegalArgumentException( "The specified filename is an existing directory" );
    }

    @Override
    public String[] getEventFilter() {
        return new String[] { VERTEX_INIT, TASK_ATMT_START, TASK_ATMT_FINISHED };
    }

    @Override
    public void processNextEvent( String callerID, HistoryEventProto event ) {
        if ( VERTEX_INIT.equals( event.getEventType() ) && isLLAP( event ) ) {
            String vertexID = event.getVertexId();
            if ( null != vertexID ) 
                vertexIDToVertexInit.put( vertexID, event );
        }
        else if ( TASK_ATMT_START.equals( event.getEventType() ) ) {
            String nodeID      = getEventValue( event, "nodeId" );
            String containerID = getEventValue( event, "containerId" );

            if ( null != event.getTaskAttemptId() ) {
                taskAtmtIDToStart.put( event.getTaskAttemptId(), new AttemptData(nodeID, containerID ) );
            }
        }
        else if ( TASK_ATMT_FINISHED.equals( event.getEventType()) ) {
            String vertexID = event.getVertexId();
            if ( null != vertexID && vertexIDToVertexInit.containsKey( vertexID ) )
                reportTaskAttemptFinish( event );
        }
    }

    @Override
    public void allEventsReceived() {
		flushLineBuffer();
    }

    /**
     * Called for every event of type "TASK_ATTEMPT_FINISHED".
     * This method is generating a single line within the report output. It essentially extracts
     * the counters from the event, adds denormalized vertex information and writes it as single
     * line to the output report.
     * 
     * @param event The task attempt finish event
     */
    private void reportTaskAttemptFinish( HistoryEventProto event ) {
        if ( !headerWritten ) {
            headerWritten = true;
            // column titles
            addLine( "time|nodeID|contID|applID|vertexID|dagID|taskID|status|" + 
                     "hdfsBytesRead|hdfsBytesWritten|hdfsReadOps|hdfsWriteOps|" +
                     "taskDurationMillis|inputRecords|inputSplitLengthBytes|createdFiles|" +
                     "allocatedBytes|allocatedUsedBytes|cacheMissBytes|consumerTimeNano|" +
                     "decodeTimeNano|hdfsTimeNano|metaDataCacheMiss|decodeBatches|vectorBatches|" + 
                     "rowsEmitted|selRowGroups|totalIONano|specQueueNano|specRunningNano|" +
                     "vertextName|vertexNumTasks" );
        }

        // directly accessible fields
        sb.setLength( 0 );

        String nodeID = "";
        String contID = "";

        AttemptData ad = taskAtmtIDToStart.get( event.getTaskAttemptId() );
        if ( null != ad ) {
            if ( null != ad.nodeID )
                nodeID = ad.nodeID;

            if ( null != ad.containerID )
                contID = ad.containerID;

            taskAtmtIDToStart.remove( event.getTaskAttemptId() );
        }

        sb.append( event.getEventTime() ).append( '|' ) 
          .append( nodeID ).append( '|' )
          .append( contID ).append( '|' )
          .append( event.getAppId() ).append( '|' ) 
          .append( event.getVertexId() ).append( '|' )
          .append( event.getDagId() ).append( '|' )
          .append( event.getTaskId() ).append( '|' )
          .append( getNonNullEventValue( event, "status" ) ).append( '|' );

        // counters from JSON event content
        try {
            JSONObject counters = (JSONObject)jsonParser.parse( getNonNullEventValue( event, "counters" ) );

            appendCounter( counters, "HDFS_BYTES_READ" );
            appendCounter( counters, "HDFS_BYTES_WRITTEN" );
            appendCounter( counters, "HDFS_READ_OPS" );
            appendCounter( counters, "HDFS_WRITE_OPS" );

            // task duration might potentially be calculated
            appendTaskDuration( event, counters );

            appendCounter( counters, "INPUT_RECORDS_PROCESSED" );
            appendCounter( counters, "INPUT_SPLIT_LENGTH_BYTES" );
            appendCounter( counters, "CREATED_FILES" );
            appendCounter( counters, "ALLOCATED_BYTES" );
            appendCounter( counters, "ALLOCATED_USED_BYTES" );
            appendCounter( counters, "CACHE_MISS_BYTES" );
            appendCounter( counters, "CONSUMER_TIME_NS" );
            appendCounter( counters, "DECODE_TIME_NS" );
            appendCounter( counters, "HDFS_TIME_NS" );
            appendCounter( counters, "METADATA_CACHE_MISS" );
            appendCounter( counters, "NUM_DECODED_BATCHES" );
            appendCounter( counters, "NUM_VECTOR_BATCHES" );
            appendCounter( counters, "ROWS_EMITTED" );
            appendCounter( counters, "SELECTED_ROWGROUPS" );
            appendCounter( counters, "TOTAL_IO_TIME_NS" );
            appendCounter( counters, "SPECULATIVE_QUEUED_NS" );
            appendCounter( counters, "SPECULATIVE_RUNNING_NS" );
        }
        catch( ParseException pe ) {
            pe.printStackTrace();
        }

        // (denormalized) vertex information
        HistoryEventProto vertexInfo = vertexIDToVertexInit.get( event.getVertexId() );
        sb.append( getNonNullEventValue( vertexInfo, "vertexName" ) ).append( '|' )
          .append( getNonNullEventValue( vertexInfo, "numTasks" ) ); 

        addLine( sb.toString() );
    } 

    /**
     * Helper to append the value of a single counter.
     * This method appends the counter value to the string buffer. It writes a "-1" as value
     * if the counter isn't found in the JSON input.
     * 
     * @param root The root JSON object with all counters
     * @param counterName The known name of the counter
     */
    private void appendCounter( JSONObject root, String counterName ) {
        Object counter = getCounterValue( root, counterName );
        sb.append( (null==counter?"":counter).toString() ).append( '|' );
    }

    /**
     * Helper to find or calcualte the task duration.
     * Depending on the source of the @c HistoryEventProto, the task duration might
     * either be a normal counter field or it needs to be calculated from base event
     * data fields. This method tries to find the counter first and falls back to
     * calculating the task duration if the counter was not found.
     * 
     * @param event The source event
     * @param counters Already extracted counters object from event's data
     */
    private void appendTaskDuration( HistoryEventProto event, JSONObject counters ) {
        // there might be a need to calculate task duration by here if source was ATS
        if ( null == getCounterValue( counters, "TASK_DURATION_MILLIS" ) ) {
            String endTime = getEventValue( event, "endTime" );
            String startTime = getEventValue( event, "startTime" );

            if ( null != endTime && null != startTime ) {
                try {
                    sb.append( Long.parseLong( endTime ) - Long.parseLong( startTime ) );
                }
                catch( Throwable thr ) {
                    // ignore conversion errors
                }
                finally {
                    sb.append( '|' );
                }
            }
            else 
                sb.append( '|' );
        }
        else
            appendCounter( counters, "TASK_DURATION_MILLIS" );
    }

    /**
     * Search and extract a given counter value.
     * This helper iterates the JSON counter hierarchy to find a counter with the given name. If
     * it is found, it returns the counter value. Otherwise it returns @c null.
     * 
     * @param src The root counter JSON object
     * @param counterName The name of the counter to search for
     * @return The found counter value or @c null
     */
    private Object getCounterValue( JSONObject src, String counterName ) {
        Object arr = src.get( "counterGroups" );
        if ( arr instanceof JSONArray ) {
            JSONArray groups = (JSONArray)arr;

            for ( Object entry : groups ) {
                if ( entry instanceof JSONObject ) {
                    JSONObject groupObj = (JSONObject)entry;
                    Object counterArr = groupObj.get( "counters" );

                    if ( counterArr instanceof JSONArray ) {
                        JSONArray counterList = (JSONArray)counterArr;
                        for ( Object counterObj : counterList ) {
                            if ( counterObj instanceof JSONObject ) {
                                JSONObject counter = (JSONObject)counterObj;
                                Object     name    = counter.get("counterName");

                                if ( null != name && counterName.equals(name) ) {
                                    return counter.get( "counterValue" );
                                }
                            }
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Adds a single line to the report file.
     * The method actually uses a line buffer and only writes lines to the buffer, once it is
     * full. This method is used to add report content.
     * 
     * @param str The line to add to the report
     */
    private void addLine( String str ) {
        checkNotNull( str, "The passed in string can't be null" );

        lineBuffer[lineBufferIdx++] = str;
        if ( lineBuffer.length <= lineBufferIdx ) 
            flushLineBuffer();
    }

    /**
     * Writes the current content of the line buffer to the report.
     * The first flush will overwrite potentially existing report files, whole all subsequent calls
     * will append to the same file. The line buffer is reset, making it available for more report
     * lines again.
     */
    private void flushLineBuffer() {
        if ( 0 < lineBufferIdx ) {
            try( PrintWriter writer = new PrintWriter( 
                new BufferedOutputStream( 
                new FileOutputStream( targetFile, hadPreviousLineBuffers))) ) {
                for ( int lIdx = 0; lIdx < lineBufferIdx; ++lIdx ) 
                    writer.println( lineBuffer[lIdx] );
            }
            catch( IOException ioe ) {
                LOG.error( "Couldn't write to report", ioe );
                System.exit( 8 );
            }

            lineBufferIdx = 0;
            hadPreviousLineBuffers = true;
        }
    }
    
    /**
     * Helper to figure out if a particular vertex initialize event is for LLAP.
     * This class deals with LLAP related output only. Whenever we find a vertex initialization
     * event, we extract the information about the scheduler and figure out if LLAP was used or
     * not. This helper is extracting this information from the event.
     * 
     * @param event The vertex initialization event
     * @return @c true if LLAP was used as task scheduler
     */
    private boolean isLLAP( HistoryEventProto event ) {
        String  svcPlugin = getEventValue( event, "servicePlugin" );

        if ( null != svcPlugin ) {
            try {
                JSONObject obj       = (JSONObject)jsonParser.parse( svcPlugin );
                String     scheduler = (String)obj.get( "taskSchedulerName" );
    
                return null != scheduler && "LLAP".equals( scheduler );
            }
            catch( ParseException pe ) {
                LOG.error( "Invalid JSON document found in servicePlugin of " + VERTEX_INIT, pe );
            }
        }

        return false;
    }

    /**
     * Returns a value from the event data list or an empty string if not found.
     * 
     * @param event The source event
     * @param name The name of the event data field to get
     * @return Either the content of the event data or an empty string
     */
    private String getNonNullEventValue( HistoryEventProto event, String name  ) {
        String ret = getEventValue( event, name );
        return ret==null?"":ret;
    }

    /**
     * Returns the value of an event data entry.
     * 
     * @param event The source event
     * @param name The key name of the event data field
     * @return The event data value or @c null if not found
     */
    private String getEventValue( HistoryEventProto event, String name ) {
        String ret = null;

        for ( KVPair kvp : event.getEventDataList() ) {
            if ( kvp.getKey().equals( name ) ) {
                ret = kvp.getValue();
                break;
            }
        }

        return ret;
    }
}