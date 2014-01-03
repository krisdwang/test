package com.amazon.messaging.seqstore.v3.internal;



import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.Checkpoint;
import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.InflightEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.MessageListener;
import com.amazon.messaging.seqstore.v3.SeqStoreReaderMetrics;
import com.amazon.messaging.seqstore.v3.StoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;
import com.amazon.messaging.seqstore.v3.internalInterface.Inflight;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.Store;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;



/**
 * Provides the flywheel facing API for SeqStore destination readers. Essentially puts together the v3 components such
 * that they can be used in a way that mimics the API presented to the end user.
 * 
 * @author kaitchuc
 */
public class SeqStoreReaderV3<InfoType> implements SeqStoreReaderV3InternalInterface<InfoType> {

    private static final Log log = LogFactory.getLog(SeqStoreReaderV3.class);

    private final Inflight<AckIdV3, InfoType> inflight;

    private final Store messages;

    private final Dequeuer<InfoType> dequeuer;

    private final StoreReader reader;

    private final StoreId storeId;

    private final String readerName;

    private final ConfigProvider<InfoType> configProvider;

    private final AckIdV3 startingAckLevel;
    
    public SeqStoreReaderV3(StoreId storeId, String readerName, 
                            SeqStoreReaderImmutableConfig<InfoType> readerConfig,
                            ConfigProvider<InfoType> configProvider,
                            CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider,
                            Store messages, AckIdV3 previousAckLevel,
                            StoreSpecificClock clock) throws SeqStoreDatabaseException, SeqStoreClosedException 
    {
        this.storeId = storeId;
        this.readerName = readerName;
        this.configProvider = configProvider;
        this.messages = messages;

        // Build the dequeue stack
        Checkpoint<AckIdV3, AckIdV3, InfoType> checkpoint = null;
        if( checkpointProvider != null ) {
            try {
                checkpoint = checkpointProvider.getCheckpointForReader(storeId, readerName);
            } catch( RuntimeException e ) {
                log.error( 
                        "Failed loading checkpoint for " + storeId + "." + readerName + 
                        ". Continuing without checkpoint", e );
            }
            if( checkpoint != null ) previousAckLevel = AckIdV3.max( previousAckLevel, checkpoint.getAckLevel() );
        }
        

        this.startingAckLevel = findStartingAckId(storeId, messages, readerConfig, previousAckLevel, clock);
        this.inflight = new InflightImpl<InfoType>(checkpoint, startingAckLevel, clock);
        this.reader = new StoreReaderImpl(messages, startingAckLevel);
        this.dequeuer = new Dequeuer<InfoType>(clock, reader, inflight, messages, readerConfig);
        
        if( checkpoint != null ) {
            this.dequeuer.advanceTo( checkpoint.getReadLevel() );
        }
    }

    

    private static AckIdV3 findStartingAckId(StoreId storeId, Store messages,
                                             SeqStoreReaderImmutableConfig<?> readerConfig, 
                                             AckIdV3 previousAckLevel, StoreSpecificClock clock) 
        throws SeqStoreClosedException 
    {
        AckIdV3 earliestAllowedAckId;
        
        long maxMessageLifetime = readerConfig.getStartingMaxMessageLifetime();
        if (maxMessageLifetime < 0) {
            earliestAllowedAckId = messages.getMinAvailableLevel();
        } else {
            long startTime = Math.max(0, clock.getStoreTime(storeId) - maxMessageLifetime);
            earliestAllowedAckId = 
                AckIdV3.max( 
                    messages.getMinAvailableLevel(),
                    new AckIdV3(startTime, false) );
        }
    
        return AckIdV3.max( earliestAllowedAckId, previousAckLevel );
    }

    @Override
    public boolean ack(AckIdV3 ackId) {
        return inflight.ack(ackId);
    }

    public void close() {
        dequeuer.close();
    }

    @Override
    public InflightEntry<AckIdV3, InfoType> dequeue() throws SeqStoreException {
        return dequeuer.dequeue();
    }
    
    @Override
    public InflightEntry<AckIdV3, InfoType> dequeueFromStore() throws SeqStoreException {
        return dequeuer.dequeueFromStore();
    }
    
    @Override
    public InflightEntry<AckIdV3, InfoType> dequeueFromInflight()
        throws SeqStoreException
    {
        return dequeuer.dequeueFromInflight();
    }

    @Override
    public AckIdV3 getReadLevel() {
        return inflight.getReadLevel();
    }

    @Override
    public InflightMetrics getInflightMetrics() {
        return inflight.getInflightMetrics();
    }

    @Override
    public int getInflightMessageCount() throws SeqStoreClosedException {
        return inflight.getInflightMessageCount();
    }

    @Override
    public SeqStoreReaderMetrics getStoreBacklogMetrics() throws SeqStoreException {
        return reader.getStoreBacklogMetrics(inflight.getAckLevel());
    }

    @Override
    public InflightEntry<AckIdV3, InfoType> getInFlightMessage(AckIdV3 ackId) throws SeqStoreException {
        InflightEntryInfo<AckIdV3, InfoType> inflightEntryInfo = inflight.getInflightEntryInfo(ackId);
        if (inflightEntryInfo == null) {
            return null;
        }

        StoredEntry<AckIdV3> storedEntry = messages.get(ackId);
        if (storedEntry == null) {
            log.info("Acking " + ackId + " in getInFlightMessage as it has been removed from disk.");
            inflight.ack(ackId);
            return null;
        }

        return new InflightEntry<AckIdV3, InfoType>(storedEntry, inflightEntryInfo);
    }

    @Override
    public String getReaderName() {
        return readerName;
    }

    @Override
    public AckIdV3 getAckLevel() {
        return inflight.getAckLevel();
    }

    /**
     * Bring the semaphore up to one because something has been enqueued. Nicer
     * than having to pass the semaphores back to the parent store after
     * construction.
     */
    @Override
    public void messageEnqueued(long availableTime) {
        dequeuer.messageEnqueued(availableTime);
    }
    
    @Override
    public void updateConfig(SeqStoreReaderImmutableConfig<InfoType> config) {
        dequeuer.updateConfig(config);
    }

    @Override
    public void updateConfig() throws SeqStoreMissingConfigException {
        try {
            updateConfig( configProvider.getReaderConfig( storeId.getGroupName(), readerName) );
        } catch (ConfigUnavailableException e) {
            throw new SeqStoreMissingConfigException("No configuration found to update " + 
                    SeqStoreV3.getConfigKey(storeId) + ":"  +  readerName, e);
        }
    }
    
    @Override
    public long getTimeOfNextMessage() throws SeqStoreException {
        return dequeuer.getTimeOfNextMessage();
    }
    
    @Override
    public long getTimeOfNextInflightMessage() {
        return dequeuer.getTimeOfNextInflightMessage();
    }
    
    @Override
    public long getTimeOfNextStoreMessage() throws SeqStoreException {
        return dequeuer.getTimeOfNextStoreMessage();
    }

    @Override
    public List<InflightEntryInfo<AckIdV3, InfoType>> getAllMessagesInFlight() throws SeqStoreClosedException {
        return inflight.getAllMessagesInFlight();
    }

    @Override
    public InflightEntryInfo<AckIdV3, InfoType> getInFlightInfo(AckIdV3 ackId) throws SeqStoreClosedException {
        return inflight.getInflightEntryInfo(ackId);
    }
    
    @Override
    public InflightUpdateResult update(AckIdV3 ackId, InflightUpdateRequest<InfoType> updateRequest) throws SeqStoreException
    {
        InflightUpdateResult result = inflight.update(ackId, updateRequest);
        if (result == InflightUpdateResult.DONE && updateRequest.isNewTimeoutSet() ) {
            dequeuer.onUpdate( updateRequest.getNewTimeout() );
        }
        return result;
    }

    @Override
    public void setAckLevel(AckIdV3 level) throws SeqStoreException {
        dequeuer.advanceTo(level);
        inflight.setAckLevel(level);
    }

    @Override
    public Checkpoint<AckIdV3, AckIdV3, InfoType> getCheckpoint() {
        return inflight.getCheckpoint(-1);
    }
    
    @Override
    public Checkpoint<AckIdV3, AckIdV3, InfoType> getPartialCheckpoint(int maxInflightMessages) {
        return inflight.getCheckpoint(maxInflightMessages);
    }

    @Override
    public boolean mergePartialCheckpoint(AckIdV3 checkpointAckLevel, AckIdV3 checkpointReadLevel, Set<AckIdV3> checkPointMessages)
            throws SeqStoreException 
    {
        boolean retval = inflight.mergePartialCheckpoint( checkPointMessages, checkpointAckLevel, checkpointReadLevel );
        
        // Move the reader forward if the checkpointReadLevel is ahead of the current read level
        dequeuer.advanceTo( checkpointReadLevel );
        
        return retval;
    }

    @Override
    public void resetInflight() {
        inflight.makeAllDeliverable();
    }

    @Override
    public SeqStoreReaderImmutableConfig<InfoType> getConfig() {
        return dequeuer.getConfig();
    }
    
    /**
     * Returns an unmodifiable sorted set (by id) of the ids of the inflight messages
     */
    @Override
    public NavigableSet<AckIdV3> getAllInflightIds() {
        return inflight.getAllInflightIds();
    }
    
    @Override
    public void setMessageListener(
        ScheduledExecutorService executor, MessageListener<AckIdV3, InfoType> listener)
    {
        dequeuer.setMessageListener( executor, listener );
    }
    
    @Override
    public Future<Void> removeMessageListener (MessageListener<AckIdV3, InfoType> listener) {
        return dequeuer.removeMessageListener (listener);
    }
    

    /**
     * Set if messages should be evicted from the cache after they've been dequeued from 
     * the store. This does not affect if messages should be cached when they are dequeued
     * from inflight. Currently messages dequeued from inflight are always cached
     * as the odds of them being read again is much higher.
     */
    @Override
    public synchronized void setEvictFromCacheAfterDequeueFromStore(boolean evict) {
        reader.setEvictFromCacheAfterReading(evict);
    }
    
    /**
     * Get if messages should be evicted from the cache after they've been dequeued.
     */
    @Override
    public synchronized boolean isEvictFromCacheAfterDequeueFromStore() {
        return reader.isEvictFromCacheAfterReading();
    }
}
