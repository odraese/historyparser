package com.cloudera.historyparser;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class of protobuffer based event readers.
 * We use parallelism to read the larger protobuffer event files, containing manifest and history
 * event information. This base class is used by these file readers. It performs the directory
 * scan for the protobuffer files and uses an executor service to read each of them. It also 
 * provides a central mechanism (@c waitForFinish) to block until all files are read.
 */
abstract class MapBase {
    private static AtomicInteger   queueLength = new AtomicInteger( 0 );  ///< number jobs, submitted to executor
    private static boolean         cancelled = false;                     ///< cancel requested by reporter
    private static ExecutorService service =                              ///< used to have parallel file read
        Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );

    protected Configuration conf = new TezConfiguration();  ///< empty/dummy configuration       

    /// private logger instance
    private final static Logger LOG = LoggerFactory.getLogger( MapBase.class );

    /**
     * Dunction interface to be notified once per second to report about the progress.
     * An instance, implementing this interface, is called every second, while the files are read.
     * It can report the current progress status and/or decide if the whole file read operation
     * should be cancelled.
     */
    @FunctionalInterface
    public interface WaitReporter {
        /**
         * Called by @c waitForFinish once per second to report progress.
         * If the function returns true, it is requesting a cancel of the actual file read
         * operation. So, along with its reporting, it gets the chance to cancel the protobuffer
         * file reading once per second.
         */
        abstract boolean reportWaitStaus();
    }

    /**
     * Scans through the directory (recursive for files and spawns file reader tasks.
     * As soon as this method is called, it performs a multithreaded read of the files within
     * the specified directory (or any subdirectory). The actual file reading/processing has to
     * be implemented by the deriving class.
     * 
     * @param path The base directory with the protobuf files
     */
    protected void init( Path path ) {
        try {
            FileSystem fs = path.getFileSystem( conf );

            if ( fs.isDirectory( path ) ) {
                RemoteIterator<LocatedFileStatus> it = fs.listFiles( path, true );

                Runnable scanTask = (new Runnable(){
                    RemoteIterator<LocatedFileStatus> fileIterator = null;

                    Runnable setIterator( RemoteIterator<LocatedFileStatus> it ) {
                        fileIterator = it;
                        return this;
                    }
                
                    @Override
                    public void run() {
                        try {
                            LOG.info( "Starting file system scan" );
                            queueLength.incrementAndGet();

                            while ( fileIterator.hasNext() ) {
                                LocatedFileStatus lfs = fileIterator.next();
            
                                synchronized( service ) {
                                    // submit each file read as parallel executable operation
                                    service.submit( (new Runnable(){
                                        private Path scanPath = null;
                
                                        Runnable setPath( Path p ) {
                                            queueLength.incrementAndGet();
                                            scanPath = p;
                                            return this;
                                        }
                                    
                                        @Override
                                        public void run() {
                                            try {
                                                if ( !cancelled ) 
                                                    processFile( scanPath );
                                            }
                                            catch( Throwable thr ) {
                                                thr.printStackTrace();
                                            }
                                            finally {
                                                queueLength.decrementAndGet();
                                            }
                                        }
                                    }).setPath( lfs.getPath() ) );
                                }
                            }

                            queueLength.decrementAndGet();
                            LOG.info( "File list scan complete." );
                        }
                        catch( IOException ioe ) {
                            LOG.error( "Failed to iterate files in file system", ioe );
                            System.exit( 8 );
                        }
                    }
                }).setIterator( it );

                synchronized( service ) {
                    service.submit( scanTask );
                }

                // wait for up to 500ms for the thread to start
                int maxLoops = 50;
                while ( queueLength.get() < 1 && maxLoops-- > 0 ) {
                    try {
                        Thread.sleep( 10 );
                    }
                    catch( InterruptedException ie ) {
                        // ignore
                    }
                }
            }
            else 
                throw new IllegalArgumentException( "The specified path is not a directory" );
        }
        catch( IOException ioe ) {
            LOG.error( "Failed to get file system for file scan", ioe );
            System.exit(8);
        }
    }    

    /**
     * Returns the file reading backlog.
     * The backlog is the total abount of tasks (files), which are scheduled and not yet finished.
     * 
     * @return The amount of running and waiting file reading tasks
     */
    public static int getQueueLength() { 
        return queueLength.get();
    }

    /**
     * Helper to wait until all file read operations are finished.
     * This method loops until the parallel background file reading operations finished. You can
     * optionally specify a @c WaitReporter instance here, which is called once per second to
     * report the progress or even to cancel the whole file reading operation.
     * 
     * @param reporter Used to report progress (or null if not required)
     */
    public static void waitForFinish( WaitReporter reporter ) {
        boolean hadOneReport = false;

        LOG.info( "Starting to wait for completion of event processing" );

        while ( 0 < queueLength.get() ) {
            try {
                Thread.sleep( 1000 );
            }
            catch( InterruptedException ie ) {
                break;
            }

            if ( null != reporter ) {
                hadOneReport = true;
                cancelled |= reporter.reportWaitStaus();
            }
        }

        // terminate the executor
        synchronized( service ) {
            service.shutdown();
            service = null;
        }

        // produce at least one output report
        if ( null != reporter && !hadOneReport )
            reporter.reportWaitStaus();
    }

    /**
     * Called (from within an asynchronous thread) to read the content of a file.
     * The deriving class has to implement this method to read the actual protobuf content
     * of the specified file.
     * 
     * @param filePath The full path to the file to read
     */
    protected abstract void processFile( Path filePath );
}