package com.hortonworks.historyparser;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;

import com.hortonworks.historyparser.processors.TaskFeatureExtractor;
import com.hortonworks.historyparser.processors.TaskTimeEventProcessor;

import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;

/**
 * Main application construct to process protobuff history event files. This
 * application might need to adjusted to the specific scenario. It is generally
 * reading the protobuf files with the help of the @c QueryMap and @c DagMap
 * classes and delegates the actual event processing to a @c EventProcessor
 * implementing class. But you might want to adjust the code to configure the
 * maximum event amount, which @c EcentProcessor to use and so on...
 */
public class App {
    private static final int PROCESSOR_ARG_IDX     = 0;    ///< program argument index for processor
    private static final int SOURCE_DIR_ARG_IDX    = 1;    ///< program argument index for source dir
    private static final int TARGET_REPORT_ARG_IDX = 2;    ///< program argument index for report

    private Path           baseDir       = null;               ///< base directory with protobuf content
    private QueryMap       queryMap      = null;               ///< content of the query_data directory
    private DagMap         dagMap        = null;               ///< content of the dag_data directory
    private EventProcessor procssor      = null;               ///< consumer for dag_data events
    private int            maxEvents     = Integer.MAX_VALUE;  ///< puts a limit on read events
    private boolean        readQueryData = true;               ///< allows skipping of query_data read

    /**
     * Creates a new application instance for a given base directory.
     * 
     * @param baseDir The directory with the protobuf source files (will be scanned recursively)
     */
    public App( Path baseDir ) {
        checkNotNull( baseDir, "Base directory cannot be null" );
        this.baseDir = baseDir;
    }

    /**
     * Allows setting the consumer/processor of @c HistoryEventProto instances.
     * The event processor is the actuall code that is looking into the event content to figure
     * out a specific scenario.
     * 
     * @oaran processor The consumer or processor of the dag_data protobuf events
     */
    public void setEventProcessor( EventProcessor processor ) {
        checkNotNull( processor, "The event processor can't be null" );
        this.procssor = processor;
    }

    /**
     * Sets the approx. limit for events, read from protobuf files.
     * This is a soft limit. The application will still finish reading the files, that it 
     * already started reading but it will not start the processing of new/queued files, once
     * that this limit is exceeded.
     * The processor is then still given the chance to process the events that were read so far,
     * so this method is simply limiting the total amount of data to be processed.
     * 
     * @param maxEvents The upper limit for events before we cancel the reading of protobuf files
     */
    public void setEventLimit( int maxEvents ) {
        this.maxEvents = maxEvents;
    }

    /**
     * The query_data directory (represented by the @c QueryMap class) is actually a manifest file,
     * telling about query execition requests. If you need to know the caller of particular tasks
     * or DAG executions, you will need to read this. If you want to focus on the @c HistoryEventProto
     * instance only, you can skip reading the manifest. The default is to read the manifest.
     * 
     * @param readIt Indicator if query_data should be read.
     */
    public void setReadQueryData( boolean readIt ) {
        readQueryData = readIt;
    }

    /**
     * Triggers the parallel read of the protobuf files.
     * The method is actually using the @c DagMap and @c QueryMap classes to read the protobuf file
     * content through an executor service for maximum parallelism. This method will block until these
     * threads finished or the maximum amount of events was read.
     */
    public void readProtoFiles() {
        checkState( null == queryMap && null == dagMap, "The proto files were already read" );

        // perform asynchronous read of all proto files in these directories 
        if ( readQueryData )
            queryMap = new QueryMap( new Path( baseDir, "query_data" ) );

        dagMap = new DagMap( new Path( baseDir, "dag_data" ), null!=procssor?procssor.getEventFilter():null );

        // wait for asynchronous read operations to finish
        if ( null != dagMap ) {
            MapBase.waitForFinish( new MapBase.WaitReporter(){
                @Override
                public boolean reportWaitStaus() {
                    StringBuffer sb = new StringBuffer();
                    int survivors = dagMap.getTotalNumberEvents() - dagMap.getFilteredEvents();
    
                    sb.append( "Remaining files: " );
                    sb.append( MapBase.getQueueLength() );
                    sb.append( ", totalDAGEvents: " );
                    sb.append( dagMap.getTotalNumberEvents() );
                    sb.append( ", filteredDAGEvents: " );
                    sb.append( dagMap.getFilteredEvents() );
                    sb.append( ", survivingDAGEvents: " );
                    sb.append( survivors );
                    sb.append( ", survivingDAGPercent: " );
                    sb.append( (survivors * 100) / dagMap.getTotalNumberEvents() );
                    sb.append( '%' );
    
                    System.out.println( sb );
                    System.out.flush();
                    return maxEvents <= survivors;
                }
            } );
        }
        else {
            MapBase.waitForFinish( null );
        }
    }

    /**
     * Iterates over all read event data and forwards these to the processor.
     * You probably want to adjust this method, if you need to deal with the query_data events
     * from the manifest. Otherwise, this method simply pumps all the events into the @c
     * EventProcessor instance.
     */
    public void iterateDAGEntries() {
        checkState( (false == readQueryData || null != queryMap) && null != dagMap, "The proto files are not read" );
        checkState( null != procssor, "No EventProcessor set yet!" );

        int deliveredEvents = 0;   ///< dont deliver more than the max

        // group by application ID
        for ( String applicationID : dagMap.getApplicationIDs() ) {
            List<HistoryEventProto> dagEvents = dagMap.getEventsForApplID( applicationID );

            for ( HistoryEventProto hpe : dagEvents ) {
                procssor.processNextEvent( applicationID, hpe );

                if ( maxEvents <= ++deliveredEvents )
                    break;
            }

            if ( maxEvents <= deliveredEvents )
                break;
        }

        // nore more events to push, start the aggregation or post processing
        procssor.allEventsReceived();
    }

    /**
     * Helper to configure the application for a specific processor.
     * This helper creates the right processor instance and configrues the application
     * for it.
     * 
     * @param args The program arguments
     * @param processor The class identifier for the processor
     */
    private void setupFor( String[] args, Class<?> processor ) {
        EventProcessor p = null;

        if ( TaskTimeEventProcessor.class == processor ) {
            p = new TaskTimeEventProcessor( args[TARGET_REPORT_ARG_IDX], false );
            setReadQueryData( false );          // query_data (QueryMap) is not needed    
        }
        else if ( TaskFeatureExtractor.class == processor ) {
            p = new TaskFeatureExtractor( args[TARGET_REPORT_ARG_IDX] );
            setReadQueryData( false );          // query_data (QueryMap) is not needed    
        }

        if ( null == p )
            throw new IllegalArgumentException( "Unsupported processor class" );

        // for debugging, you can limit the amount of data to read
        // setEventLimit( 1000 );
        setEventProcessor( p );
    }

    /**
     * Application main entry point.
     * The application expects two arguments. The first one is the base directory, hosting the
     * query_data and dag_data subdirectories. This can either be local or on HDFS. The second
     * argument is the name of the local report text file to be generated.
     * 
     * @param args The program arguments
     */
    public static void main( String[] args ) {
        long startTime = System.currentTimeMillis();

        if ( 3 > args.length ) {
            System.err.println( "Usage: App <processorClassName> <pathToSourceFileDir> <reportName>" );
            System.exit(8);
        }

        Path basePath = new Path( args[SOURCE_DIR_ARG_IDX] );
        App  app      = new App( basePath );

        // find the processor class and configure it
        String processorName = EventProcessor.class.getPackage().getName() 
                               + ".processors." + args[PROCESSOR_ARG_IDX];

        try {
            Class<?> processor = Class.forName( processorName );
            app.setupFor( args, processor );
        }
        catch( ClassNotFoundException cnfe ) {
            System.err.println( "Unknown event processor: " + args[0] );
        }
        
        System.out.println( "Starting to read protofiles..." );
        app.readProtoFiles();   // we don't need queryData for this scenario

        System.out.println( "Iterating protofile content..." );
        app.iterateDAGEntries();
        
        System.out.println();
        System.out.println( "Done (with a total of " + app.dagMap.getTotalNumberEvents() + 
                            " events) in " + ((System.currentTimeMillis() - startTime) / 1000) + "s." );
    }
}
