package com.amazon.messaging.seqstore.v3;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

/**
 * Performs dequeueing from the persistent message store and peeks. Only one
 * thread should have access to this. The Store's iterator maintains state and
 * should be thread safe, wrt the store
 */
public interface StoreReader {

    /**
     * Obtain the next unread entry from the store, and advance the following
     * entry.
     * @throws SeqStoreException 
     */
    StoredEntry<AckIdV3> dequeue() throws SeqStoreException;

    /**
     * Obtain the next unread entry from the store.
     * @throws SeqStoreException 
     */
    StoredEntry<AckIdV3> peek() throws SeqStoreException;

    void close();

    AckIdV3 getPosition();
    
    /**
     * Move the cursor forward to pos if it is not already beyond pos
     * 
     * @param level
     * @throws SeqStoreClosedException 
     * @throws SeqStoreException 
     */
    AckIdV3 advanceTo(AckIdV3 pos) throws SeqStoreDatabaseException, SeqStoreClosedException;

    SeqStoreReaderMetrics getStoreBacklogMetrics(AckIdV3 ackLevel) throws SeqStoreException;
    
    /**
     * Set if entries should be evicted from the cache after they have been read
     */
    void setEvictFromCacheAfterReading(boolean evict);
    
    /**
     * Return if entries should be evicted from the cache after they have been read
     */
    boolean isEvictFromCacheAfterReading();
}
