package com.amazon.messaging.seqstore.v3.internalInterface;

import java.util.NavigableSet;
import java.util.Set;

import com.amazon.messaging.seqstore.v3.Checkpoint;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.ReaderInflightData;

/**
 * An internal interface for the SeqStoreReader for V3. This class adds functions
 * to the interface that should only be used within the v3 SequentialStorage
 * and Flywheel implementations.
 * 
 * @author stevenso
 *
 */
public interface SeqStoreReaderV3InternalInterface<InfoType> extends SeqStoreReader<AckIdV3, InfoType> {
    public AckIdV3 getAckLevel() throws SeqStoreClosedException;

    public void messageEnqueued(long availableTime) throws SeqStoreClosedException;

    /**
     * Set the ack level of the reader. This will only move the ack level forwards 
     * not backwards. If the ack level has already passed the specified level
     * the update will be ignored.
     * <p>  
     * If the ack level is moved forward any messages below the specified level
     * will be treated as acked and removed from the inflight table. The 
     * cursor will also be moved forward to the specified level if it 
     * is not already past that level.
     * 
     * @param level
     * @throws SeqStoreException 
     */
    public void setAckLevel(AckIdV3 level) throws SeqStoreException;

    /**
     * Merges a partial checkpoint with the inflight state of this reader. See 
     * {@link ReaderInflightData#mergePartialCheckpoint(Set, AckIdV3, AckIdV3, long)} for details
     * on how the merge works.
     *
     * @param ackLevel the ack level to merge with the readers ack level
     * @param readLevel the read level to merge with the readers read level.
     * @param messsages a set of inflight messages to merge with the known set of inflight messages
     * @return boolean true if this checkpoint is out of date - that is if reader knows of messages that have 
     *   been acked that are recorded as being inflight by the partial checkpoint, false otherwise
     * @throws SeqStoreException 
     */
    public boolean mergePartialCheckpoint(AckIdV3 ackLevel, AckIdV3 readLevel, Set<AckIdV3> messages) 
        throws SeqStoreException;
    
    /**
     * Get the time the next message in the inflight table will be available.
     * @throws SeqStoreClosedException 
     */
    public long getTimeOfNextInflightMessage() throws SeqStoreClosedException;
    
    /**
     * Returns a message from the inflight for the store if one is available.
     * 
     * @return the entry for a message or null if no message in inflight is currently available
     */
    InflightEntry<AckIdV3, InfoType> dequeueFromInflight() throws SeqStoreException;
    
    /**
     * Get the time of the next message available from the store, ignoring the inflight
     * @throws SeqStoreException
     */
    public long getTimeOfNextStoreMessage() throws SeqStoreException;
    
    /**
     * Returns a message from the store, ignoring the inflight messages
     * 
     * @return the entry for a message or null if no message is currently available from the store
     */
    public InflightEntry<AckIdV3, InfoType> dequeueFromStore() throws SeqStoreException;

    public AckIdV3 getReadLevel() throws SeqStoreClosedException;

    public void resetInflight() throws SeqStoreClosedException;

    /**
     * Get a checkpoint for the reader with a limited number of inflight messages. If this checkpoint
     * is used to restore the state any messages after the first maxInflightMessages messages may
     * be redelivered.
     * 
     * @param maxInflightMessages the maximum number of inflight messages to include in the checkpoint
     * @return a checkpoint 
     * @throws SeqStoreException
     */
    public Checkpoint<AckIdV3, AckIdV3, InfoType> getPartialCheckpoint(int maxInflightMessages) throws SeqStoreException;
    
    @Override
    public Checkpoint<AckIdV3, AckIdV3, InfoType> getCheckpoint() throws SeqStoreException;
    
    /**
     * Returns an unmodifiable sorted set (by id) of the ids of the inflight messages
     * @throws SeqStoreClosedException 
     */
    public NavigableSet<AckIdV3> getAllInflightIds() throws SeqStoreClosedException;
    
    /**
     * Update this destinations config from using the provided config.
     * 
     * @throws SeqStoreClosedException 
     */
    public void updateConfig(SeqStoreReaderImmutableConfig<InfoType> config) throws SeqStoreClosedException;
    

    /**
     * Set if messages should be evicted from the cache after they've been dequeued from 
     * the store. This does not affect if messages should be cached when they are dequeued
     * from inflight. Currently messages dequeued from inflight are always cached
     * as the odds of them being read again is much higher.
     * @throws SeqStoreClosedException 
     */
    public void setEvictFromCacheAfterDequeueFromStore(boolean evict) throws SeqStoreClosedException;
    
    /**
     * Get if messages should be evicted from the cache after they've been dequeued.
     */
    public boolean isEvictFromCacheAfterDequeueFromStore() throws SeqStoreClosedException;
}
