package com.cloudera.historyparser;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;

import com.cloudera.historyparser.processors.TaskFeatureExtractor;
import com.cloudera.historyparser.processors.TaskTimeEventProcessor;

import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application construct to process protobuff history event files. This
 * application might need to adjusted to the specific scenario. It is generally
 * reading the protobuf files with the help of the @c QueryMap and @c DagMap
 * classes and delegates the actual event processing to a @c EventProcessor
 * implementing class. But you might want to adjust the code to configure the
 * maximum event amount, which @c EcentProcessor to use and so on...
 */
public class App {
    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    private Path baseDir = null; /// < base directory with protobuf content
    private QueryMap       queryMap      = null;               ///< content of the query_data directory
    private DagMap         dagMap        = null;               ///< content of the dag_data directory
    private ATSMap         atsMap        = null;
    private EventProcessor procssor      = null;               ///< consumer for dag_data events
    private int            maxEvents     = Integer.MAX_VALUE;  ///< puts a limit on read events
    private boolean        readQueryData = true;               ///< allows skipping of query_data read
    private boolean        useStreaming  = false;              ///< pass events directly to processor

    /**
     * Creates a new application instance for a given base directory.
     * 
     * @param baseDir The directory with the protobuf source files (will be scanned recursively)
     */
    public App( Path baseDir ) {
        checkNotNull( baseDir, "Base directory cannot be null" );
        LOG.info( "Starting history-parser in {}", baseDir.toString() );

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

        LOG.info( "Setting event processor {}", processor.getClass().getSimpleName() );
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
        LOG.info( "Limiting input to {} events.", maxEvents );
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

        LOG.info( "{} reading of query_data.", readIt?"Enabled":"Disabled" );
    }

    /**
     * Enables streaming of events rather then storing in the @c DagMap.
     * Streaming can only be used if the query_data events are not required.
     * 
     * @param streamIt the new streaming state
     */
    public void setStreaming( boolean streamIt ) {
        checkArgument( !streamIt || !readQueryData , "Can't use streaming with QueryData read" );
        useStreaming = streamIt;

        LOG.info( "Switched streaming mode {}", streamIt?"on":"off" );
    }

    /**
     * Triggers the parallel read of the protobuf files.
     * The method is actually using the @c DagMap and @c QueryMap classes to read the protobuf file
     * content through an executor service for maximum parallelism. This method will block until these
     * threads finished or the maximum amount of events was read.
     */
    public void readProtoFiles() {
        checkState( null == queryMap && null == dagMap, "The proto files were already read" );

        LOG.info( "Start reading the ProtoBuf files" );

        // perform asynchronous read of all proto files in these directories 
        if ( readQueryData )
            queryMap = new QueryMap( new Path( baseDir, "query_data" ) );

        if ( useStreaming ) 
            dagMap = new DagMap( new Path( baseDir, "dag_data" ), procssor );
        else
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
                    if ( 0 < dagMap.getTotalNumberEvents() ) {
                        sb.append( ", survivingDAGPercent: " );
                        sb.append( (survivors * 100) / dagMap.getTotalNumberEvents() );
                        sb.append( '%' );
                    }
    
                    LOG.info( sb.toString() );
                    return maxEvents <= survivors;
                }
            } );
        }
        else {
            MapBase.waitForFinish( null );
        }
    }

    /**
     * Triggers the reading of ATS (dag) event data.
     * If the data is supplied via JSON ATS files, rather than a sequence file of HistoryEventProto
     * instances, then this method is used to utilize the @c ATSMap bridge, which converts the
     * JSON files to HistoryEventProto instances. This potentially allows the reuse of the
     * event processors - even if right now only @c TaskFeatureExtractor is supported for ATS.
     */
    private void readATSFiles() {
        checkState( null == atsMap , "The ATS data has already been read" );
        checkState( useStreaming, "ATS mode currently supports on streaming processing" );

        LOG.info( "Start reading the ATS files" );

        atsMap = new ATSMap( baseDir, procssor );

        MapBase.waitForFinish( new MapBase.WaitReporter(){
            @Override
            public boolean reportWaitStaus() {
                StringBuffer sb = new StringBuffer();
                int survivors = atsMap.getTotalNumberEvents() - atsMap.getFilteredEvents();

                sb.append( "foundFiles: " );
                sb.append( atsMap.getFoundFiles() );
                sb.append( ", qLength: " );
                sb.append( MapBase.getQueueLength() );
                sb.append( ", totalDAGEvents: " );
                sb.append( atsMap.getTotalNumberEvents() );
                sb.append( ", filteredDAGEvents: " );
                sb.append( atsMap.getFilteredEvents() );
                sb.append( ", survivingDAGEvents: " );
                sb.append( survivors );
                if ( 0 < atsMap.getTotalNumberEvents() ) {
                    sb.append( ", survivingDAGPercent: " );
                    sb.append( (survivors * 100) / atsMap.getTotalNumberEvents() );
                    sb.append( '%' );
                }

                LOG.info( sb.toString() );
                return maxEvents <= survivors;
            } } );
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
        LOG.info( "Start processing of DAG events" );

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
     * @param parsedArgs The program arguments
     * @param processor The class identifier for the processor
     */
    private void setupFor( ProgramArgs parsedArgs, Class<?> processor ) {
        EventProcessor p = null;

        if ( TaskTimeEventProcessor.class == processor ) {
            p = new TaskTimeEventProcessor( parsedArgs.targetReport, false );
            setReadQueryData( false );          // query_data (QueryMap) is not needed    
        }
        else if ( TaskFeatureExtractor.class == processor ) {
            p = new TaskFeatureExtractor( parsedArgs.targetReport );
            setReadQueryData( false );          // query_data (QueryMap) is not needed    
            setStreaming( true );               // pass events directly to processor
        }

        if ( null == p )
            throw new IllegalArgumentException( "Unsupported processor class" );

        // for debugging, you can limit the amount of data to read
        if ( 0 < parsedArgs.limitEvents ) 
            setEventLimit( parsedArgs.limitEvents );

        setEventProcessor( p );
    }

    /**
     * Helper construct to parse the program arguments.
     * This helper iterates the program arguments and places them into the accessible
     * member variables. 
     */
    private static class ProgramArgs {
        String  processorName   = null;    ///< name of the event processor
        String  sourceDirectory = null;    ///< source directory (after -i option)
        String  targetReport    = null;    ///< target report (after -o option)
        boolean useATS          = false;   ///< optional indicator for -ats option
        int     limitEvents     = 0;       ///< optional content of -l limit option

        /**
         * Takes the program arguments and splits them into the member variables.
         * 
         * @param args The program arguments.
         */
        public ProgramArgs( String[] args ) {
            for ( int i = 0; i < args.length; ++i ) {
                if ( args[i].startsWith( "-i" ) ) 
                    sourceDirectory = getValue( args, i++ );
                else if ( args[i].startsWith( "-o" ) ) 
                    targetReport = getValue( args, i++ );
                else if ( args[i].equalsIgnoreCase( "-ats" ) )
                    useATS = true;
                else if ( args[i].startsWith( "-l" ) ) 
                    limitEvents = Integer.parseInt( getValue( args, i++ ) );
                else if ( null == processorName ) 
                    processorName = args[i];
                else {
                    LOG.error( "Invalid or unsupported parameter: {}", args[i] );
                    LOG.error( "Usage: <ProcessorName> -i <inputFileDirectory> -o <reportFileName> [-ats] [-l <limitEventVal>" );
                    System.exit( 8 );
                }
            }

            if ( null == processorName ) {
                LOG.error( "Missing processorName parameter" );
                System.exit( 8 );
            }

            if ( null == sourceDirectory ) {
                LOG.error( "Missing source file directory" );
                System.exit( 8 );
            }

            if ( null == targetReport ) {
                LOG.error( "Missing target report name" );
                System.exit( 8 );
            }
        }

        /**
         * Delivers the value of a program option.
         * We support a -x=value or -x value notation for the arguments that specify a
         * value like the source directory. This helper figures out which of the notations
         * is used and returns the value.
         * 
         * @param args The source program argument
         * @param idx The positition, where the -x portion was found in args
         */
        private String getValue( String[] args, int idx ) {
            String ret = null;

            String source = args[idx];
            if ( 4 < source.length() ) {
                if ( source.charAt( 2 ) == '=' ) {
                    ret = source.substring( 3 );
                }
            }

            if ( null == ret ) {
                if ( idx + 1 < args.length ) {
                    source = args[idx +1];
                    if ( !source.startsWith( "-" ) ) 
                        ret = source;
                }
            }

            return ret;
        }
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
        ProgramArgs parsedArgs = new ProgramArgs( args );
        long startTime = System.currentTimeMillis();

        Path basePath = new Path( parsedArgs.sourceDirectory );
        App  app      = new App( basePath );

        // find the processor class and configure it
        String processorName = EventProcessor.class.getPackage().getName() 
                               + ".processors." + parsedArgs.processorName;

        try {
            Class<?> processor = Class.forName( processorName );
            app.setupFor( parsedArgs, processor );
        }
        catch( ClassNotFoundException cnfe ) {
            LOG.error( "Unknown event processor: {}", args[0] );
        }
        
        if ( parsedArgs.useATS ) {
            app.readATSFiles();

            LOG.info( "Done (with a total of {} events) in {}s." , 
                      app.atsMap.getTotalNumberEvents(), 
                      (System.currentTimeMillis() - startTime) / 1000 );
        }
        else {
            app.readProtoFiles();   // we don't need queryData for this scenario

            if ( !app.useStreaming ) {
                app.iterateDAGEntries();
            }
            
            LOG.info( "Done (with a total of {} events) in {}s." , 
                      app.dagMap.getTotalNumberEvents(), 
                      (System.currentTimeMillis() - startTime) / 1000 );
        }
    }
}
