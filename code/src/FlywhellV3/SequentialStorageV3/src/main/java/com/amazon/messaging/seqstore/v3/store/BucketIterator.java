/**
 *
 */
package com.amazon.messaging.seqstore.v3.store;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An iterator for moving forward through messages in a bucket. 
 * 
 * Note that any iterator moved beyond the enqueue level in
 * the bucket will not have a reliable position.
 *
 */
public interface BucketIterator {

    /**
     * Advance the iterator and return the next entry in the bucket. If there are no more entries in the
     * bucket the iterator is not advanced and the function returns null.
     * 
     * @return the next entry in the bucket or null if there is no next entry or the bucket has been deleted
     * @throws SeqStoreDatabaseException if there was a problem accessing the storage layer
     */ 
    public StoredEntry<AckIdV3> next() throws SeqStoreDatabaseException;

    /**
     * Advance to the entry with the highest ackid that is equal to or below id and
     * return its id. Returns null if there are no messages below the specified level. Attempting
     * to move backwards will leave the cursor where it is and return the current ackid.  
     * 
     * @param id the id to advance up to.
     * @return the ackid of the message the iterator stopped on or null if there are no
     *   entries below id in the bucket or the if the bucket has been deleted.
     * @throws SeqStoreDatabaseException if there was a problem accessing the storage layer
     */
    public AckIdV3 advanceTo(AckIdV3 id) throws SeqStoreDatabaseException;
    
    /**
     * Advance to the at the given position. If the iterator is passed the position
     * nothing is changed.
     * 
     * @param position the position to advance to
     * @return the ackid of the message the iterator stopped on or null if there are no
     *   entries below id in the bucket or the if the bucket has been deleted.
     * @throws SeqStoreDatabaseException if there was a problem accessing the storage layer
     * @throws IllegalArgumentException if position is not in the bucket or references a position that
     *   does not exist
     */
    public AckIdV3 advanceTo(StorePosition position) 
            throws SeqStoreDatabaseException, IllegalArgumentException;

    /**
     * Get the position of the iterator. For iterators beyond the enqueue level
     * the position may not reflect insertions done after the iterator moved
     * beyond the enqueue level.
     * @throws SeqStoreDatabaseException 
     */
    public StorePosition getPosition() throws SeqStoreDatabaseException;
    
    /**
     * Return the id identifying the bucket this iterator is for.
     */
    public AckIdV3 getBucketId();
    
    /**
     * Return the key that was returned by the last call to advanceTo or next. Returns null
     * if advanceTo or next have never been called.
     * 
     * @return
     */
    @Nullable
    public AckIdV3 currentKey();
    
    /**
     * Set if entries should be evicted from the cache after they have been read
     */
    public void setEvictFromCacheAfterReading(boolean evict);
    
    /**
     * Return if entries should be evicted from the cache after they have been read
     */
    public boolean isEvictFromCacheAfterReading();
}
