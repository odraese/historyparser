package com.hortonworks.historyparser;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.ManifestEntryProto;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;

/**
 * Reader for @c HistoryEventProto events, stored in dag_data.
 * This class is used to read all available dag_data files and extract the @c HistoryEventProto
 * events. The events are accessbile by applicationID (as specified in each event) or by their
 * caller (callerID from the DAG_SUBMITTED event).
 */
public class DagMap extends MapBase {
    private ConcurrentHashMap<String, String>                       dagID2CallerID = null;  ///< maps DAG id to caller 
    private ConcurrentHashMap<String, ArrayList<HistoryEventProto>> callerID2Event = null;  ///< maps callerID to event list
    private ConcurrentHashMap<String, ArrayList<HistoryEventProto>> applID2Event   = null;  ///< maps application ID to event list
    private AtomicInteger   totalNumberEvents   = null;  ///< counts all read events
    private AtomicInteger   eventsWithoutCaller = null;  ///< counts the events for which we don't find a caller in dagID2CallerID
    private AtomicInteger   filteredEvents      = null;  ///< amount of events removed by event type filtering
    private HashSet<String> filterEventTypes    = null;  ///< list of event types to include

    /**
     * Creates a new reader for the dag_data directory content.
     * The so created instance will immediately start reading the directory and all files that
     * are in it (including in subdirectries). It does so through the @c MapBase class, which
     * spawns multiple threads, each processing a single file. After constructing an instance
     * of this class, you should use the @c Map.waitForFinish to determine when all files are
     * read. Reading the protobuf files is usually the more expensive part of the analysis.
     * 
     * @param path The path to the dag_data directory to read.
     * @param filters A list of event types to include (or null to include all)
     */
    public DagMap(Path path, String[] filters) {
        dagID2CallerID = new ConcurrentHashMap<>();
        callerID2Event = new ConcurrentHashMap<>();
        applID2Event = new ConcurrentHashMap<>();
        totalNumberEvents = new AtomicInteger(0);
        eventsWithoutCaller = new AtomicInteger(0);
        filteredEvents = new AtomicInteger( 0 );

        if ( null != filters ) {
            filterEventTypes = new HashSet<>();

            for ( String filter : filters ) {
                checkArgument( null != filter, "Can't specify null in filters" );
                checkArgument( 0 < filter.trim().length(), "Filter can't be empty string" );
                filterEventTypes.add( filter );
            }
        }

        init( path );  // kick off the reading of the directory content
    }

    /**
     * Returns all events, associated indirectly with a caller.
     * The caller is specified through the application ID of a manifest entry.
     * 
     * @param entry The manifest entry (from @c QueryMap)
     * @return The list of events, associated with the caller
     */
    public List<HistoryEventProto> getEventsForCaller( ManifestEntryProto entry ) {
        return getEventsForCaller( entry.getAppId() );  // entry's appID is our caller
    }

    /**
     * Returns all events, associated indirectly with a caller.
     * The caller is specified through the application ID of the caller as string.
     * 
     * @param entry The caller's application ID 
     * @return The list of events, associated with the caller
     */
    public List<HistoryEventProto> getEventsForCaller( String callerID ) {
        return callerID2Event.get( callerID );
    }

    /**
     * Returns all events for a particular application ID.
     * Each @c HistoryEventProto is tagged with an application ID and this reader also groups
     * these events by this ID. With this method, you can get all event instances of the specified
     * application ID.
     * 
     * @param applID The application ID for which to get all event entries
     * @return The list of associated event instances
     */
    public List<HistoryEventProto> getEventsForApplID( String applID ) {
        return applID2Event.get( applID );
    }

    /**
     * @return A @c Set of all known/registered caller identifiers
     */
    public Set<String> getCallerIDs() {
        return callerID2Event.keySet();
    }

    /**
     * @return A @c Set of all known/registered application identifiers (from the events)
     */
    public Set<String> getApplicationIDs() {
        return applID2Event.keySet();
    }

    /**
     * @return The total amount of events, read from all files.
     */
    public int getTotalNumberEvents() {
        return totalNumberEvents.get();
    }

    /**
     * @return number of events for which we couldn't associate a caller (normally DAG stop events)
     */
    public int getEventsWithoutCaller() {
        return eventsWithoutCaller.get();
    }

    /**
     * @return the amount of evenets that didn't qualify the filter (filtered out)
     */
    public int getFilteredEvents() {
        return filteredEvents.get();
    }

    @Override
    protected void processFile( Path path ) {
        try {
            DatePartitionedLogger<HistoryEventProto> hiveEventLogger = 
                new DatePartitionedLogger<>(HistoryEventProto.PARSER, path, conf, new SystemClock() );

            ProtoMessageReader<HistoryEventProto> eventReader =  hiveEventLogger.getReader( path );
            HistoryEventProto event = null;
            while( null != (event = eventReader.readEvent()) ) {
                String callerID = dagID2CallerID.get( event.getDagId() );

                totalNumberEvents.incrementAndGet();

                if ( null == callerID ) {
                    if ( "DAG_SUBMITTED".equals( event.getEventType() ) ) {
                        for ( int edIdx = 0; edIdx < event.getEventDataCount(); ++edIdx ) {
                            if ( "callerId".equals( event.getEventData( edIdx ).getKey() ) ) {
                                callerID = event.getEventData( edIdx ).getValue();
                                dagID2CallerID.putIfAbsent( event.getDagId(), callerID );

                                break;
                            }
                        }
                    }
                }

                if ( null != callerID ) {
                    if ( null == filterEventTypes || filterEventTypes.contains( ( event.getEventType() ) ) ) {
                        // map the callerID (matching entry from QueryMap) to event list
                        ArrayList<HistoryEventProto> eventList = callerID2Event.get( callerID );
                        if ( null == eventList ) {
                            eventList = new ArrayList<>();
                            ArrayList<HistoryEventProto> prev = callerID2Event.putIfAbsent( callerID, eventList );
                            if ( null != prev ) 
                                eventList = prev;
                        }
        
                        synchronized( eventList ) {
                            eventList.add( event );
                        }

                        // also map applicationID to list of events
                        if ( null != event.getAppId() && 0 < event.getAppId().trim().length() ) {
                            eventList = applID2Event.get( event.getAppId() );
        
                            if ( null == eventList ) {
                                eventList = new ArrayList<>();
                                ArrayList<HistoryEventProto> prev = applID2Event.putIfAbsent( event.getAppId(), eventList );
                                if ( null != prev )
                                    eventList = prev;
                            }
        
                            synchronized( eventList ) {
                                eventList.add( event );
                            }
                        }
                    }
                    else 
                        filteredEvents.incrementAndGet();
                }
                else
                    eventsWithoutCaller.incrementAndGet();
            }
        }
        catch( EOFException eof ) {
            // ignore
        }
        catch( IOException ioe ) {
            ioe.printStackTrace();
            System.exit( 4 );
        }
    }
}