package com.hortonworks.historyparser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.ManifestEntryProto;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;

/**
 * Reader for @c ManifestEntryProto events, stored in query_data.
 * This class is used to read all available query_data files and extract the @c ManifestEntryProto
 * events. The events are accessbile by applicationID (as specified in each event).
 */
class QueryMap extends MapBase {
    private ConcurrentHashMap<String,ArrayList<ManifestEntryProto>> eventsByApp = null;  ///< application ID to event list

    /**
     * Creates a new reader for the query_data directory content.
     * The so created instance will immediately start reading the directory and all files that
     * are in it (including in subdirectries). It does so through the @c MapBase class, which
     * spawns multiple threads, each processing a single file. After constructing an instance
     * of this class, you should use the @c Map.waitForFinish to determine when all files are
     * read. Reading the protobuf files is usually the more expensive part of the analysis.
     * 
     * @param path The path to the query_data directory to read.
     */
    public QueryMap( Path path ) {
        eventsByApp = new ConcurrentHashMap<>();
        init( path );
    }    

    /**
     * @return A @c Set of all known/registered application identifiers (from the events)
     */
    public Set<String> getApplicationIDs() {
        return eventsByApp.keySet();
    }

    /**
     * @return The manifest entries/events for a particular application.
     */
    public List<ManifestEntryProto> getManifestEntries( String applicationID ) {
        return eventsByApp.get( applicationID );
    }

    @Override
    protected void processFile( Path filePath ) {
        try {
            DatePartitionedLogger<ManifestEntryProto> manifestEventsLogger = 
                new DatePartitionedLogger<>(ManifestEntryProto.PARSER, filePath, conf, new SystemClock() );
            ProtoMessageReader<ManifestEntryProto> manifestReader = 
                manifestEventsLogger.getReader( filePath );
            ManifestEntryProto mevent = null;

            while( null != (mevent = manifestReader.readEvent() ) ) {
                ArrayList<ManifestEntryProto> protoList = eventsByApp.get( mevent.getAppId() );

                if ( null == protoList ) {
                    protoList = new ArrayList<>();
                    eventsByApp.put( mevent.getAppId(), protoList );
                }

                protoList.add( mevent );
            }
        }
        catch( IOException ioe ) {
            // ignore
        }
    }
}