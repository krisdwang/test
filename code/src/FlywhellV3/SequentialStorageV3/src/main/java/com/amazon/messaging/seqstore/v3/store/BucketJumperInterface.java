package com.amazon.messaging.seqstore.v3.store;

import net.jcip.annotations.NotThreadSafe;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

/**
 * An iterator that is not limited to a single bucket. 
 * 
 * @author kaitchuc
 */
@NotThreadSafe
public interface BucketJumperInterface {

    /**
     * Move to the next entry and return it. If there are no more entries the iterator is not moved 
     * and the function returns null.
     * 
     * @return the next entry or null if there are no more entries.
     * @throws SeqStoreException if there was a problem accessing the storage layer or the store was closed
     */
    public StoredEntry<AckIdV3> next() throws SeqStoreException;

    /**
     * Advances to the the closest ackid equal to or below id and returns that ackid. If there are no records
     * below id this function returns null. If id is below the current level of the iterator it the iterator does 
     * not move and the ackid of the current level is returned.
     * 
     * @throws SeqStoreClosedException if the store was closed
     * @throws SeqStoreDatabaseException if there was a problem accessing the storage layer 
     */
    public AckIdV3 advanceTo(AckIdV3 id) throws SeqStoreDatabaseException, SeqStoreClosedException;

    /**
     * Get the ackid of the current entry the iterator points to, or null if the iterator
     * does not point to any key.
     * 
     * @return the ackid the entry the iterator currently points to or null if the
     *  iterator does not point to any entry.
     */
    public AckIdV3 getCurKey();

    /**
     * Close the iterator freeing all resources associated with the iterator. No
     * other methods are safe to call on the iterator after close is called. 
     */
    public void close();
    
    /**
     * Create a copy of this iterator. The new iterator points to the same
     * entry this iterator points to.
     * @return a copy of this.
     * @throws SeqStoreException if there was a problem accessing the storage layer or the store was closed
     */
    public BucketJumperInterface copy() throws SeqStoreException;

    /**
     * Get the position of the iterator. For iterators beyond the enqueue level
     * the position may not reflect insertions done after the iterator moved
     * beyond the enqueue level.
     * @throws SeqStoreDatabaseException 
     */
    public StorePosition getPosition() throws SeqStoreDatabaseException;
    
    /**
     * Set if entries should be evicted from the cache after they have been read
     */
    public void setEvictFromCacheAfterReading(boolean evict);
    
    /**
     * Return if entries should be evicted from the cache after they have been read
     */
    public boolean isEvictFromCacheAfterReading();
}
