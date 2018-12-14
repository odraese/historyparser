package com.hortonworks.historyparser;

import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;

/**
 * Defines a processor for the events, read and delegated by the @c App class.
 * An event processor can be a specific scenario, analyzing the events of the history log.
 */
public interface EventProcessor {
    /**
     * Returns a list of history event types, used to filter the incoming events.
     * Events are only passed to @c processNextEvent if their type is listed by this method.
     * An implementing class therefore has to specify the events it is interested in. It can
     * also return @c null to get all, unfiltered content.
     * 
     * @return An array of the event types, that should be processed
     */
    String[] getEventFilter();

    /**
     * Called by the @c App class for each qualifying event.
     * The @c App class is forwarding qualifying events to the processor, event by event. The
     * first parameter is the callerID (the applicationID as found in query_data, matching the)
     * corrsponding DAG.
     * 
     * @param callerID The caller (query application ID)
     * @param event The actual event data
     */
    void processNextEvent( String callerID, HistoryEventProto event );

    /**
     * Notification that all events are received.
     * The implementing class can use this method to perform the actual processing of all the
     * events that were delivered via the @c processNextEvent method. No further events will
     * be added to the processor after this method has been called.
     */
    void allEventsReceived();
}