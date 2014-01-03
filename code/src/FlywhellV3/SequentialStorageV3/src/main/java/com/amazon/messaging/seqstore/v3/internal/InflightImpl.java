package com.amazon.messaging.seqstore.v3.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.inflight.core.StripeLockTable;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.Checkpoint;
import com.amazon.messaging.seqstore.v3.InflightEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightInfoFactory;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.internal.ReaderInflightData.RedeliverySchedule;
import com.amazon.messaging.seqstore.v3.internalInterface.Inflight;
import com.amazon.messaging.utils.Pair;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of inflight. This implementation uses {@link ReaderInflightData} to store the inflight
 * data for each reader and a {@link StripeLockTable} to manage synchronization of the ReaderInflightData
 * objects and their creation and removal. 
 */
public class InflightImpl<InfoType> implements Inflight<AckIdV3, InfoType> {
    private static final Log log = LogFactory.getLog(InflightImpl.class);

    private final Clock clock_;
    
    private final ReaderInflightData<InfoType> readerInflightData;

    
    /**
     * Create a new InflightImpl.
     * 
     * @param checkpoint the checkpoint for to use to start the reader, ignored if null
     * @param ackLevel the ackLevel to start with
     * @param clock the clock to base message redelivery on. Should be a local clock not a remote one.
     */
    public InflightImpl(Checkpoint<AckIdV3, AckIdV3, InfoType> checkpoint, AckIdV3 ackLevel, Clock clock) {
        clock_ = clock;
        if( checkpoint != null ) {
            readerInflightData = new ReaderInflightData<InfoType>(checkpoint);
        } else {
            readerInflightData = new ReaderInflightData<InfoType>();
        }
        
        // Do this even if there is a checkpoint as the ack level may have advanced after the checkpoint was made
        readerInflightData.setAckLevel(ackLevel);
    }


    @Override
    public void add(AckIdV3 ackId, InfoType inflightInfo, int timeout)
        throws IllegalStateException 
    {
        synchronized( readerInflightData ) {
            readerInflightData.add(ackId, clock_.getCurrentTime() + timeout, inflightInfo);
        }
    }

    @Override
    public InflightUpdateResult update(AckIdV3 ackId, InflightUpdateRequest<InfoType> updateRequest) {
        long currentTime = clock_.getCurrentTime();
        
        synchronized( readerInflightData ) {
            return readerInflightData.update(ackId, currentTime, updateRequest);
        }
    }

    /**
     * We can be lazy about updating the greatestAckedAckId This means that it
     * may be less than it really is. This is OK, since it means that the
     * cleanup thread will just run a little behind.
     */
    @Override
    public boolean ack(AckIdV3 ackId) {
        if (ackId == null) {
            throw new IllegalArgumentException("ackId was null");
        }

        synchronized( readerInflightData ) {
            return readerInflightData.ack( ackId );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public AckIdV3 getAckLevel() {
        synchronized( readerInflightData ) {
            return readerInflightData.getAckLevel();
        }
    }

    @Override
    public InflightEntryInfo<AckIdV3, InfoType> peekNextRedeliverable() {
        synchronized( readerInflightData ) {
            Pair<RedeliverySchedule, InfoType> nextRedeliverable =
                    readerInflightData.peekFirstScheduled(clock_.getCurrentTime());

            return nextRedeliverable == null ? null
                                             : new InflightEntryInfo<AckIdV3, InfoType>(
                                                     nextRedeliverable.getKey().getAckId(),
                                                     nextRedeliverable.getValue(),
                                                     nextRedeliverable.getKey().getRedeliveryTime() - clock_.getCurrentTime());
        }
    }

    @Override
    public long getNextRedeliveryTime() {
        synchronized( readerInflightData ) {
            return readerInflightData.getTimeOfNextTimeout();
        }
    }

    @Override
    public InflightEntryInfo<AckIdV3, InfoType> dequeueMessage(InflightInfoFactory<InfoType> infoFactory) {
        if( infoFactory == null ) {
            throw new IllegalArgumentException("infoFactory was null");
        }

        long currentTime = clock_.getCurrentTime();

        // find a timed out message to dequeue, and dequeue it.
        // loop until a message is successfully dequeued, or there
        // are no remaining timed out messages to re-deliver.
        Pair<RedeliverySchedule, InfoType> entryToDequeue;
        InfoType infoAfterDequeue;
        int timeoutAfterDequeue;
        InflightUpdateResult dequeueResult;
        do {
            synchronized( readerInflightData ) {
                entryToDequeue = readerInflightData.takeFirstScheduled(currentTime);
                if (entryToDequeue == null) {
                    return null;
                }
            }

            infoAfterDequeue = entryToDequeue.getValue();	        
            timeoutAfterDequeue = 0;
            try {
                infoAfterDequeue = infoFactory.getMessageInfoForDequeue(
                        entryToDequeue.getValue());
                timeoutAfterDequeue = infoFactory.getTimeoutForDequeue(
                        infoAfterDequeue);
            } catch (Exception exc) {
                log.error("InflightInfoFactory failed to calculate re-delivery timeout and/or new inflight info for dequeued message: "
                        + entryToDequeue, exc);
            } finally {
                InflightUpdateRequest<InfoType> updateRequest = new InflightUpdateRequest<InfoType>()
                        .withExpectedInflightInfo(entryToDequeue.getValue())
                        .withNewInflightInfo(infoAfterDequeue)
                        .withNewTimeout(Math.max(0, timeoutAfterDequeue));
                synchronized( readerInflightData ) {
                    dequeueResult = readerInflightData.update(
                            entryToDequeue.getKey().getAckId(),
                            currentTime, 
                            updateRequest );
                }
            }
        } while (dequeueResult != InflightUpdateResult.DONE);

        return new InflightEntryInfo<AckIdV3, InfoType>(
                entryToDequeue.getKey().getAckId(),
                infoAfterDequeue,
                timeoutAfterDequeue);
    }

    @Override
    public InflightEntryInfo<AckIdV3, InfoType> getInflightEntryInfo(AckIdV3 ackId) {
        if (ackId == null) {
            throw new IllegalArgumentException("ackId was null");
        }
        synchronized( readerInflightData ) {
            Pair<RedeliverySchedule, InfoType> entry =
                    readerInflightData.getInflightEntry(ackId);
            return entry == null ? null
                                 : new InflightEntryInfo<AckIdV3, InfoType>(
                                         entry.getKey().getAckId(),
                                         entry.getValue(),
                                         entry.getKey().getRedeliveryTime() - clock_.getCurrentTime());
        }
    }

    /*
     * O(|inflight|)
     */
    @Override
    public InflightMetrics getInflightMetrics() {
        synchronized( readerInflightData ) {
            return readerInflightData.getMetrics( clock_.getCurrentTime() );
        }
    }

    @Override
    public int getInflightMessageCount() {
        synchronized( readerInflightData ) {
            return readerInflightData.size();
        }
    }

    @Override
    public long setAckLevel(AckIdV3 level) {
        if (level == null) {
            throw new IllegalArgumentException("ackId was null");
        }
        
        synchronized( readerInflightData ) {
            return readerInflightData.setAckLevel( level );
        }
    }

    @Override
    public void makeAllDeliverable() {
        synchronized( readerInflightData ) {
            readerInflightData.makeAllMessagseDeliverable( clock_.getCurrentTime() );
        }
    }

    @Override
    public List<InflightEntryInfo<AckIdV3, InfoType>> getAllMessagesInFlight() {
        List<Pair<RedeliverySchedule, InfoType>> entries;
        synchronized( readerInflightData ) {
            entries = readerInflightData.getAllEntries();
        }

        if (entries.isEmpty()) {
            return Collections.emptyList();
        } else {
            long currentTime = clock_.getCurrentTime();
            List<InflightEntryInfo<AckIdV3, InfoType>> messages =
                new ArrayList<InflightEntryInfo<AckIdV3,InfoType>>(entries.size());
            for (Pair<RedeliverySchedule, InfoType> entry : entries) {
                messages.add( new InflightEntryInfo<AckIdV3, InfoType>(
                        entry.getKey().getAckId(),
                        entry.getValue(),
                        entry.getKey().getRedeliveryTime() - currentTime));
            }
            return messages;
        }
    }
    
    @Override
    public Checkpoint<AckIdV3, AckIdV3, InfoType> getCheckpoint(int maximumInflightMessages) {
        synchronized( readerInflightData ) {
            return readerInflightData.getCheckpoint(maximumInflightMessages);
        }
    }

    @Override
    public boolean mergePartialCheckpoint(Set<AckIdV3> messages,  
                                          AckIdV3 checkpointAckLevel, AckIdV3 checkpointReadLevel) 
    {
        synchronized( readerInflightData ) {
            return readerInflightData.mergePartialCheckpoint( 
                    messages, checkpointAckLevel, checkpointReadLevel, clock_.getCurrentTime() );
        }
    }
    
    @Override
    public NavigableSet<AckIdV3> getAllInflightIds() {
        synchronized( readerInflightData ) {
            return readerInflightData.getAllInflightIds();
        }
    }
    
    @Override
    public AckIdV3 getReadLevel() {
        synchronized( readerInflightData ) {
            return readerInflightData.getReadLevel();
        }
    }

    @Override
    public Clock getClock() {
        return clock_;
    }
}
