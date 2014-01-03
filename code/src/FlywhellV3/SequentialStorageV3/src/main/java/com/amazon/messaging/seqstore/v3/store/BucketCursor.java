package com.amazon.messaging.seqstore.v3.store;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public interface BucketCursor {
    /**
     * Call to close the cursor. The cursor cannot be used after this is called.
     */
    void close() throws SeqStoreDatabaseException;
    
    /**
     * Return a copy of this bucket cursor that will start at the same position.
     */
    BucketCursor copy() throws SeqStoreDatabaseException;
    
    /**
     * Advance to the highest entry less than or equal to the given key. 
     * Returns the distance between the old and new positions.
     * 
     * @param key key to advance to
     * @return the distance between current the key.
     */
    long advanceToKey(AckIdV3 key) throws SeqStoreDatabaseException;
    
    /**
     * Jump to a the closest key less than or equal to the specified key in the database. This is much
     * cheaper than advanceToKey for long distances but does not give the distance between
     * the starting point and the current point.
     * 
     * @param key
     * @throws SeqStoreDatabaseException
     * @returns true if the coreAckId of key exists, false if it does not.
     */
    boolean jumpToKey(AckIdV3 key) throws SeqStoreDatabaseException;
    
    /**
     * Advance to the next entry (with key less than or equal to max if max is not null) and return it if present. 
     * If there is no next key returns null.
     *   
     * @param max if non-null the maximum possible key for the entry to return
     * @return the next entry or null if there is no next key
     */
    StoredEntry<AckIdV3> nextEntry(AckIdV3 max) throws SeqStoreDatabaseException;
    
    /**
     * Return the current key this cursor is pointing to. This will be the key of the last record returned by nextEntry
     * or nextKey. This function is the only function that it is still safe to call after the cursor has been
     * closed.
     */
    AckIdV3 current();
    
    /**
     * Get the number of entries between the the current position(exclusive) and key(inclusive).
     * 
     * @param key the key to measure the distance from. If null the distance is measured from the start of the bucket.
     */
    long getDistance(AckIdV3 key) throws SeqStoreDatabaseException;
    
    /**
     * Set if entries should be evicted from the cache after they have been read
     */
    void setEvictFromCacheAfterReading(boolean evict);
    
    /**
     * Return if entries should be evicted from the cache after they have been read
     */
    boolean isEvictFromCacheAfterReading();
}
