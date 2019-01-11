package com.cloudera.historyparser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.KVPair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converter from ATS JSON data to @c HistoryEventProto.
 * If the DAG data is provided as ATS directory, where each task execution is described by
 * JSON formatted event data in multiple files, this class is used to read these JSON files
 * and convert them into @c HistoryEventProto instances to allow subsequent processing with
 * existing @c EventProcessor instances. So, this is a bridge between ATS and the proto based
 * processing.
 */
class ATSMap extends MapBase {
    /// event type for vertx initialization 
    private static final String VERTEX_INIT = "VERTEX_INITIALIZED";

    /// event type for the start of a task execution attempt
    private static final String TASK_ATMT_START = "TASK_ATTEMPT_STARTED";

    /// event type for the finish of a task execution attempt
    private static final String TASK_ATMT_FINISH = "TASK_ATTEMPT_FINISHED";

    /// logger for this class
    private final static Logger LOG = LoggerFactory.getLogger(ATSMap.class);

    private ThreadLocal<JSONParser> localParser = null;      ///< shared JSON parser
    private ThreadLocal<FileSystem> localFileSystem = null;  ///< hadoop filesystem instance
    private EventProcessor streamProcessor = null;           ///< target (stream) processor 
    private AtomicInteger foundFiles = null;                 ///< total amount of found files
    private AtomicInteger totalEvents = null;                ///< counts all incoming events
    private AtomicInteger filteredEvents = null;             ///< counts all LLAP events

    /**
     * POJO data class for the fields of the event header.
     */
    private static class EventHeader {
        String type;       ///< event type identifier
        long   timestamp;  ///< event time 

        /**
         * Creates a new data container instance.
         * 
         * @param type The event type identifier
         * @param timestamp The event unix time
         */
        public EventHeader( String type, long timestamp ) {
            this.type = type;
            this.timestamp = timestamp;
        }
    }

    /**
     * Creates a new instance to bridge between ATS data and the event processors.
     * The so created instance will immediately start to scan for files in the provided
     * path. The constructor call might actually take as long as the whole file system scan.
     * 
     * @param path The source ATS root path
     * @param ep The target stream event processor to receive HistoryEventProto instances
     */
    public ATSMap( Path path, EventProcessor ep ) {
        checkNotNull( path );
        checkNotNull( ep );

        streamProcessor = ep;
        foundFiles = new AtomicInteger();
        totalEvents = new AtomicInteger();
        filteredEvents = new AtomicInteger();

        localParser = ThreadLocal.withInitial( () -> new JSONParser() );
        localFileSystem = ThreadLocal.withInitial( new Supplier<FileSystem>() {
            @Override
            public FileSystem get() {
                try {
                    return path.getFileSystem( new Configuration() );
                }
                catch( IOException ioe ) {
                    LOG.error( "Can't get file system!", ioe );
                }
        
                return null;
            }
        } );

        init( path );
    }

    /**
     * Delivers the total amounf of read JSON events.
     * 
     * @return Total number of events
     */
    public int getTotalNumberEvents() {
        return totalEvents.get();
    }

    /**
     * Delivers the number of qualifying eventd.
     * 
     * @return Amount of events, sent to stream processor
     */
    public int getFilteredEvents() {
        return filteredEvents.get();
    }

    /**
     * Returns the amount of ATS entitylog files.
     * 
     * @return The number of entitylog-* files in ATS directory.
     */
    public int getFoundFiles() {
        return foundFiles.get();
    }

    @Override
    protected void processFile(Path filePath) {
        if ( filePath.getName().startsWith( "entitylog-" ) ) {
            foundFiles.incrementAndGet();

            try {
                if ( localFileSystem.get().isFile( filePath ) ) {
                    try (LineNumberReader lnr = new LineNumberReader( 
                                                new InputStreamReader( 
                                                    localFileSystem.get().open( filePath ) )  ) ) {
                        String line;
    
                        while( null != (line = lnr.readLine()) ) {
                            for ( JSONObject o : splitMultipleObjcts( line ) ) {
                                handleSourceEvent( o );
                            }
                        }
                    }
                }
            }
            catch( IOException ioe ) {
                LOG.error( "Error when opening entity file", ioe );
            }
        }		
    }

    /**
     * Helper JSON preprocessor to find JSON object instances.
     * Sometimes, we see multiple JSON objects on a single line without comma or array brackets,
     * which is invalid JSON source. This helper will find the beginning and end of the outer
     * JSON objects and invokes the JSON parser individually on each of them. The returned list
     * then contains the found instances (most times just one).
     * 
     * @param all The line with one or multiple JSON objects
     * @return The list of found JSONObject instances
     */
    private List<JSONObject> splitMultipleObjcts( String all ) {
        ArrayList<JSONObject> ret      = new ArrayList<>();
        int                   lvl      = 0;
        int                   startIdx = 0;

        // scan through the line, character by character
        for ( int i = 0; i < all.length(); ++i ) {
            char c = all.charAt( i );

            if ( '{' == c ) {         // start of JSON object
                if ( 1 == ++lvl ) 
                    startIdx = i; 
            }
            else if ( '}' == c ) {    // end of JSON object
                if ( 0 == --lvl ) {   // is the outer object complete?
                    try {
                        Object obj = localParser.get().parse( all.substring( startIdx, i + 1 ) );
                        if ( obj instanceof JSONObject ) 
                            ret.add( (JSONObject)obj );
                        else 
                            LOG.warn( "JSONParser did not return object" );
                    }
                    catch( ParseException pe ) {
                        LOG.warn( "Wasn't able to parse single JSON instance", pe );
                    }
                }
                else if ( 0 > lvl ) {
                    throw new IllegalStateException();
                }
            }
        }

        return ret;
    }

    /**
     * Handles a single JSON source event.
     * Each JSON object represents an event, having an event type attribute.
     * This helper processes the JSON event and converts it to HistoryEventProto before
     * passing it on to the stream processor.
     * 
     * @param obj The source JSON object
     */
    private void handleSourceEvent( JSONObject obj ) {
        EventHeader header = getEventHeader( obj );

        totalEvents.incrementAndGet();

        // we process only vertex init, and task attempt start/finish
        if ( VERTEX_INIT.equals( header.type )     || 
             TASK_ATMT_START.equals( header.type ) ||
             TASK_ATMT_FINISH.equals( header.type )   ) {
            HistoryEventProto.Builder b = HistoryEventProto.newBuilder()
                                                           .setEventType( header.type )
                                                           .setEventTime( header.timestamp );
            addIdents( obj, b );
            b.addAllEventData( createEventData( obj ) );

            synchronized( streamProcessor ) {
                streamProcessor.processNextEvent( null, b.build() );        
            }

            filteredEvents.incrementAndGet();
        }
    }

    /**
     * Filers out the primary filters from the JSON object and sets them as properties.
     * The qualifying identifiers are transported as primary filters in the JSON object. This
     * helper copies them into the HistoryEventProto properties.
     * 
     * @param source The JSON source event object
     * @param target The builder for the HistoryEventProto
     */
    private void addIdents( JSONObject source, HistoryEventProto.Builder target ) {
        String vertexID = getFilter( source, "TEZ_VERTEX_ID" );
        if ( null != vertexID ) 
            target.setVertexId( vertexID );

        String dagID = getFilter( source, "TEZ_DAG_ID" );
        if ( null != dagID ) 
            target.setDagId( dagID );

        String applID = getFilter( source, "applicationId" );
        if ( null != applID ) 
            target.setAppId( applID );

        String taskID = getFilter( source, "TEZ_TASK_ID" );
        if ( null != taskID )
            target.setTaskId( taskID );

        Object entType = source.get( "entitytype" );    
        Object ent     = source.get( "entity" );
        if ( null != entType && entType instanceof String &&
             null != ent     && ent     instanceof String    ) {
            switch( (String)entType ) {
                case "TEZ_TASK_ATTEMPT_ID":
                    target.setTaskAttemptId( (String)ent );
                    break;
                case "TEZ_VERTEX_ID":
                    target.setVertexId( (String)ent );
                    break;
            }
        }
    }

    /**
     * Helper to copy JSON "otherinfo" into event data.
     * The content of the "otherinfo" object within the JSON source, is essentially copied 
     * property by property into the event data key/value pairs of the target @c 
     * HistoryEventProto instance. This helper converts the "otherinfo" object into a list
     * of key/value pairs.
     * 
     * @param source The source JSON object (containing otherinfo)
     * @return A list of key/value event data pairs
     */
    private List<KVPair> createEventData( JSONObject source ) {
        ArrayList<KVPair> ret = new ArrayList<>();
        Object oiObj = source.get( "otherinfo" );

        if ( null != oiObj && oiObj instanceof JSONObject ) {
            JSONObject oi = (JSONObject)oiObj;
            for ( Object key : oi.keySet() ) {
                KVPair newPair = KVPair.newBuilder()
                                       .setKey( (String)key )
                                       .setValue( oi.get( key ).toString() )
                                       .build();

                ret.add( newPair );
            }
        }
        else 
            LOG.warn( "No otherinfo found on event source." );

        return ret;
    }

    /**
     * Helper to extract the event header into a POJO object.
     * 
     * @param obj The source JSON object
     * @return An instance of EventHeader (POJO data class)
     */
    private EventHeader getEventHeader( JSONObject obj ) {
        Object eObj = obj.get( "events" );
        String type = "UNKNOWN";
        long   time = 0;

        if ( null != eObj && eObj instanceof JSONArray ) {
            JSONArray arr = (JSONArray)eObj;

            if ( 1 == arr.size() ) {
                Object eventObj = arr.get( 0 );
                if ( null != eventObj && eventObj instanceof JSONObject ) {
                    JSONObject theEvent  = (JSONObject)eventObj;
                    Object     eventType = theEvent.get( "eventtype" );
                    Object     eventTime = theEvent.get( "timestamp" );

                    if ( null != eventType ) 
                        type = eventType.toString();
                    else 
                        LOG.warn( "eventType not found in events object" );

                    if ( null != eventTime && eventTime instanceof Long ) 
                        time = ((Long)eventTime).longValue();
                    else
                        LOG.warn( "eventTime not found in events object" );
                }
            }
            else {
                LOG.warn( "Unexpected array size for events: %s", arr.size() );
            }
        
        }
        else {
            LOG.warn( "Events object not found!" );
        }

        return new EventHeader( type, time );
    }

    /**
     * Helper to find and extract a single primary filter entry.
     * The helper is used to find the primary filters in the passed in JSON source and
     * to finds the specific filter (which for some reason is encapsulated in an array)
     * and return its content.
     * 
     * @param obj The source JSON object
     * @param name The name of the primary filter to find and return
     * @return The filter content or @c null if it was not found
     */
    private String getFilter( JSONObject obj, String name ) {
        Object primFilters = obj.get( "primaryfilters" );
        String ret         = null;

        if ( null != primFilters && primFilters instanceof JSONObject ) {
            JSONObject pf = (JSONObject)primFilters;
            Object     fa = pf.get( name );

            if ( null != fa && fa instanceof JSONArray ) {
                JSONArray filterArr = (JSONArray)fa;

                if ( 1 == filterArr.size() ) {
                    Object entry = filterArr.get( 0 );
                    if ( null != entry ) 
                        ret = entry.toString();
                    else 
                        LOG.warn( "null found as filter entry for {}", name );
                }
                else 
                    LOG.warn( "Unexpected filter count for filter {}.", name );
            }
        }
        else 
            LOG.warn( "primaryFilters not found." );

        return ret;
    }
}