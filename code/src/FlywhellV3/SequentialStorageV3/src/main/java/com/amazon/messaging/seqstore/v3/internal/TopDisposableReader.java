package com.amazon.messaging.seqstore.v3.internal;



import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.Checkpoint;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.InflightEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.MessageListener;
import com.amazon.messaging.seqstore.v3.SeqStoreReaderMetrics;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.google.common.util.concurrent.Futures;



/**
 * This intercepter is responsible for ensuring mutual exclusion of a thread disposing a reader from all threads
 * invoking other methods on it.
 * 
 * @author poojag
 */
class TopDisposableReader<InfoType> implements SeqStoreReaderV3InternalInterface<InfoType> {
    private static final Log log = LogFactory.getLog(TopDisposableReader.class);

    private final SeqStoreReaderV3<InfoType> next;

    private final DisposalCoordinator dc_ = new DisposalCoordinator();

    private volatile boolean isDisposed_ = false;

    public TopDisposableReader(SeqStoreReaderV3<InfoType> next) {
        this.next = next;
    }

    /*
     * (non-Javadoc)
     * @see
     * com.amazon.messaging.seqstore.SeqStoreReader#ack(com.amazon.messaging
     * .seqstore.BinaryData)
     */
    @Override
    public final boolean ack(AckIdV3 ackId) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot ack through a disposed reader "
                        + getReaderName());
            }
            return next.ack(ackId);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    public final void close() {
        dc_.waitToDispose();
        try {
            if (isDisposed()) {
                if (log.isDebugEnabled()) {
                    log.debug("Already disposed reader " + getReaderName());
                }
                return;
            }
            next.close();
            markAsDisposed();
        } finally {
            dc_.wakupThreadsAfterDisposal();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.amazon.messaging.seqstore.SeqStoreReader#dequeue(long)
     */
    @Override
    public final InflightEntry<AckIdV3, InfoType> dequeue() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot dequeue from  a disposed reader "
                        + getReaderName());
            }
            return next.dequeue();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public InflightEntry<AckIdV3, InfoType> dequeueFromInflight() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot dequeue from  a disposed reader "
                        + getReaderName());
            }
            return next.dequeueFromInflight();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public InflightEntry<AckIdV3, InfoType> dequeueFromStore() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot dequeue from  a disposed reader "
                        + getReaderName());
            }
            return next.dequeueFromStore();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    /*
     * (non-Javadoc)
     * @see
     * com.amazon.messaging.seqstore.SeqStoreReader#getInFlightMessage(com.amazon
     * .messaging.seqstore.BinaryData)
     */
    @Override
    public final InflightEntry<AckIdV3, InfoType> getInFlightMessage(AckIdV3 ackId) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot getInflightMessage from a disposed reader "
                        + getReaderName());
            }
            return next.getInFlightMessage(ackId);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.amazon.messaging.seqstore.SeqStoreReader#getReaderName()
     */
    @Override
    public final String getReaderName() {
        // this method does not access or modify state that can be disposed
        return next.getReaderName();
    }

    @Override
    public InflightEntryInfo<AckIdV3, InfoType> getInFlightInfo(AckIdV3 ackId)
    throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getInFlightInfo(ackId);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public InflightUpdateResult update(AckIdV3 ackId, InflightUpdateRequest<InfoType> updateRequest) 
            throws SeqStoreException
    {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call update on a disposed reader " + getReaderName());
            }
            return next.update(ackId, updateRequest);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public AckIdV3 getAckLevel() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getAckLevel();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public void messageEnqueued(long availableTime) throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            next.messageEnqueued(availableTime);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public void updateConfig() throws SeqStoreClosedException, SeqStoreMissingConfigException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            next.updateConfig();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    
    @Override
    public void updateConfig(SeqStoreReaderImmutableConfig<InfoType> config) throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            next.updateConfig(config);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public InflightMetrics getInflightMetrics() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getInflightMetrics();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public int getInflightMessageCount() throws SeqStoreException, SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getInflightMessageCount();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public SeqStoreReaderMetrics getStoreBacklogMetrics() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getStoreBacklogMetrics();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public void setAckLevel(AckIdV3 level) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            next.setAckLevel(level);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public boolean mergePartialCheckpoint(AckIdV3 ackLevel, AckIdV3 readLevel, Set<AckIdV3> messages) 
        throws SeqStoreException 
    {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.mergePartialCheckpoint(ackLevel, readLevel, messages);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public long getTimeOfNextInflightMessage() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getTimeOfNextInflightMessage();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public long getTimeOfNextStoreMessage() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getTimeOfNextStoreMessage();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
   
    @Override
    public long getTimeOfNextMessage() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getTimeOfNextMessage();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
   

    @Override
    public List<InflightEntryInfo<AckIdV3, InfoType>> getAllMessagesInFlight()
    throws SeqStoreException 
    {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getAllMessagesInFlight();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public AckIdV3 getReadLevel() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getReadLevel();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public void resetInflight() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            next.resetInflight();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public SeqStoreReaderImmutableConfig<InfoType> getConfig() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getConfig();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public Checkpoint<AckIdV3, AckIdV3, InfoType> getCheckpoint() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getCheckpoint();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public Checkpoint<AckIdV3, AckIdV3, InfoType> getPartialCheckpoint(int maxInflightMessages) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getPartialCheckpoint(maxInflightMessages);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
   
    @Override
    public NavigableSet<AckIdV3> getAllInflightIds() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.getAllInflightIds();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public void setMessageListener(
        ScheduledExecutorService executor, MessageListener<AckIdV3, InfoType> listener) throws SeqStoreClosedException
    {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            next.setMessageListener(executor, listener);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public Future<Void> removeMessageListener (MessageListener<AckIdV3, InfoType> listener) {
        dc_.waitToExecuteNonDisposalCall ();
        try {
            if (!isDisposed ()) {
                return next.removeMessageListener (listener);
            }
            return Futures.immediateFuture (null);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall ();
        }
    }
    

    /**
     * Set if messages should be evicted from the cache after they've been dequeued from 
     * the store. This does not affect if messages should be cached when they are dequeued
     * from inflight. Currently messages dequeued from inflight are always cached
     * as the odds of them being read again is much higher.
     * @throws SeqStoreClosedException 
     */
    @Override
    public synchronized void setEvictFromCacheAfterDequeueFromStore(boolean evict)
            throws SeqStoreClosedException 
    {
        dc_.waitToExecuteNonDisposalCall ();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            next.setEvictFromCacheAfterDequeueFromStore(evict);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall ();
        }
    }
    
    /**
     * Get if messages should be evicted from the cache after they've been dequeued.
     * @throws SeqStoreClosedException 
     */
    @Override
    public synchronized boolean isEvictFromCacheAfterDequeueFromStore() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall ();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot call for a disposed reader " + getReaderName());
            }
            return next.isEvictFromCacheAfterDequeueFromStore();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall ();
        }
    }    

    
    // //////////////////////////////////////////////////////
    // Method(s) for Flywheel sub-classes
    // //////////////////////////////////////////////////////

    protected final boolean isDisposed() {
        return isDisposed_;
    }

    private final void markAsDisposed() {
        isDisposed_ = true;
    }
}
