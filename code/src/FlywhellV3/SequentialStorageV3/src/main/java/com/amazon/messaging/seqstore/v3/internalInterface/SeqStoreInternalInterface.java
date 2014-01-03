package com.amazon.messaging.seqstore.v3.internalInterface;

import java.util.Set;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.AckId;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.EnqueuesDisabledException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreIllegalStateException;
import com.amazon.messaging.seqstore.v3.internal.AckIdSource;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

public interface SeqStoreInternalInterface<InfoType> extends SeqStore<AckIdV3, InfoType> {
    /**
     * @return the config key used to get the store config from the config provider.
     */
    public String getConfigKey();
    
    /**
     * Overriden to make the return type more specific.
     */
    @Override
    public SeqStoreReaderV3InternalInterface<InfoType> createReader(String readerId) 
            throws SeqStoreException;
    
    /**
     * Overriden to make the return type more specific.
     */
    @Override
    public SeqStoreReaderV3InternalInterface<InfoType> getReader(String readerId)
            throws SeqStoreClosedException;
    
    /**
     * Close the store if it has been empty for at least <code>minimumEmptyTime</code> milliseconds.
     * 
     * @param minimumEmptyTime the minimum amount of time that the store must have been empty for
     *   it to be deleted.
     * @return true if the store was closed, false otherwise
     * @throws SeqStoreException
     */
    public boolean closeIfEmpty(long minimumEmptyTime) throws SeqStoreException;

    public Object getMBean() throws SeqStoreClosedException;
    
    public long getNumBuckets() throws SeqStoreClosedException;
    
    public long getNumDedicatedBuckets() throws SeqStoreClosedException;
    
    /**
     * Get an AckId to be used {@link #enqueue(AckId, Entry, long, Metrics)}. No reader will be able to
     * dequeue messages after the returned ackId until {@link #enqueueFinished(AckId)} is
     * called for the returned AckId.
     * 
     * @param requestedTime the requested time for the AckId. The returned AckId will have
     * a time that is greater than or equal to the requested time.
     * @return a new AckId to be used with {@link #enqueue(AckId, Entry, long, Metrics)}
     * @throws EnqueuesDisabledException if the store is quiesced
     * @throws SeqStoreClosedException 
     */
    public AckIdV3 getAckIdForEnqueue(long requestedTime) throws EnqueuesDisabledException, SeqStoreClosedException;
    
    /**
     * Record that an enqueue has finished. This allows readers to advance beyond the provided ackId.
     * This is only needed for AckIds retrieved using {@link #getAckIdForEnqueue(long)}. If 
     * {@link #enqueue(Entry, long, Metrics)} is used there is no need to call this function 
     * 
     * @param ackId
     * @throws SeqStoreClosedException 
     */
    public void enqueueFinished(AckIdV3 ackId) throws SeqStoreClosedException;
    
    /**
     * Enqueue a message with an externally provided ack id. The provided Id will be checked using 
     * {@link AckIdSource#recordNewId(AckIdV3)}. The {@link #enqueueFinished(AckId)} must
     * be called with the same id passed to this function before dequeues can advance beyond that id. 
     * 
     * @param ackId
     * @param message
     * @param timeout
     * @param cache should the message be kept in cache or immediately evicted
     * @param metrics
     * @throws SeqStoreException
     * @throws SeqStoreIllegalStateException if the provided ackId is not valid according to 
     *   {@link AckIdSource#recordNewId(AckIdV3)}
     */
    public void enqueue(
            AckIdV3 ackId, Entry message, long timeout,
            boolean cache, Metrics metrics) 
        throws SeqStoreException, SeqStoreIllegalStateException;
    
    /**
     * Get the minimum id for any enqueues that are in progress or that could be started in the future. 
     * All readers are blocked from advancing beyond this level so that they do not miss messages.
     * 
     * @return
     * @throws SeqStoreClosedException 
     */
    public AckIdV3 getMinEnqueueLevel() throws SeqStoreClosedException;
    
    /**
     * Get the ackid source for the store.
     * 
     * @return the ackid source for the store.
     * @throws SeqStoreClosedException
     */
    public AckIdSource getAckIdSource() throws SeqStoreClosedException;
    
    /**
     * Get the id for the store.
     */
    public StoreId getStoreId();

    /**
     * Return the an AckId that is the lower bound on messages currently available. All messages
     * below this level are unavailable and are eligible for deletion.
     */
    public AckIdV3 getMinAvailableLevel() throws SeqStoreClosedException;
    
    /**
     * Update this stores config to match the specified config.
     * 
     * @param config the config for the store
     * @param requiredReaders the set of required readers for the store
     * 
     * @throws SeqStoreClosedException 
     * @throws SeqStoreException if a reader had to be created and the creation failed 
     */
    public void updateConfig( SeqStoreImmutableConfig config, Set<String> requiredReaders ) 
        throws SeqStoreClosedException, SeqStoreException;
    
    /**
     * Are enqueues for this store cached. This is intended for debugging
     */
    @TestOnly
    public boolean isCacheEnqueues();
}
