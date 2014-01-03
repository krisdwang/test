/**
 *
 */
package com.amazon.messaging.seqstore.v3.store;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A block of messages in the store. This class is responsible for persisting
 * them and creating iterators that will be used to access them. The reason to
 * break the store into separate buckets is to allow them to be deleted, thus
 * dropping messages enmass.
 * 
 * @author kaitchuc, stevenso
 */
public interface Bucket {
    static enum BucketState {
        OPEN, CLOSED, DELETED
    }
    
    /**
     * Return a specific entry from the bucket.
     * @throws SeqStoreDatabaseException if there is a database error retrieving the item
     * @throws IllegalStateException if the bucket is not open
     */
    StoredEntry<AckIdV3> get(AckIdV3 key) throws SeqStoreDatabaseException;

    /**
     * Put a stored entry into the database.
     * 
     * @param key the ack id for the entry
     * @param value the entry to store
     * @param cache if the entry should be cached for deques
     * @throws SeqStoreDatabaseException if there is a database exception writing the message
     * @throws IllegalStateException if the bucket is not open
     */
    void put(AckIdV3 key, StoredEntry<AckIdV3> value, boolean cache) throws SeqStoreDatabaseException;

    /**
     * Return a iterator starts at the beginning
     * @throws SeqStoreDatabaseException 
     * @throws IllegalStateException if the bucket is not open
     */
    BucketIterator getIter() throws SeqStoreDatabaseException;
    
    /**
     * Copy an iterator. Returns a new iterator that is an exact copy of the provided iterator including
     * position and current key. 
     * 
     * @return a new iterator that starts at the same place as iter
     * @throws SeqStoreDatabaseException if there is a database error
     * @throws IllegalStateException if the bucket is not open or if iter has already been closed
     */
    BucketIterator copyIter( BucketIterator iter ) throws SeqStoreDatabaseException, IllegalStateException;

    /**
     * @return The name of the store this bucket is in. This function may be safely called after the bucket has been closed.
     */
    StoreId getStoreName();
    
    /**
     * Returns the number of entries in this bucket. This may be innaccurate after restart. This function
     * may be safely called after the bucket has been closed.
     * <p>
     * @return the number of entries in this bucket.
     */
    long getEntryCount();

    /**
     * Returns the size in bytes this bucket is approximately taking up. This is defined as the sum
     * of the payload sizes and ack id sizes of all messages in the bucket. This may not be
     * accurate after a restart. This function may be safely called after the bucket has been closed.
     *  
     * @return the size in bytes this bucket is approximately taking up. 
     */
    long getByteCount();

    /**
     * Close the bucket release all resources related to it. After this function is called
     * any attempt to use any iterator on the bucket or access any metadata about the
     * bucket may throw an {@link IllegalStateException}.
     * 
     * @throws SeqStoreException 
     */
    void close() throws SeqStoreDatabaseException;
    
    /**
     * Mark the bucket as deleted. This releases the same resources as close does, but still 
     * allows access to the bucket meta data. After this is called any attempt to use
     * an iterator on this bucket will throw {@link BucketDeletedException}.
     * <p>
     * It is not necessary to call close after calling deleted but it is not an error to do so.
     */
    void deleted() throws SeqStoreDatabaseException;
    
    /**
     * Get the metadata to persist for this bucket. This is safe to call even after the bucket has been closed.
     * If a store has never been created for this bucket then this will return null.
     */
    @CheckForNull
    BucketPersistentMetaData getMetadata();
    
    /**  
     * Return the bucket's current state. This function may be safely called after the bucket has been closed.
     */
    BucketState getBucketState();

    /**
     * close the iterator on this bucket
     * @throws SeqStoreDatabaseException 
     */
    void closeIterator(BucketIterator iter) throws SeqStoreDatabaseException;
    
    /**
     * Returns the number of iterators that are active on this bucket. For a closed or deleted bucket
     * this will return 0.
     */
    int getNumIterators();

    /**
     * Returns the key of this bucket - also represents the minimum possible value for last id.
     * This function may be safely called after the bucket has been closed.
     */
    AckIdV3 getBucketId();
    
    /**
     * Get the storage type used for this bucket. If no storage has been created for the bucket
     * this is the type that will be requested from the persistence manager when the bucket
     * store is created.
     * 
     * @return the storage type used to store messages for the bucket
     */
    @Nullable
    BucketStorageType getBucketStorageType();
    
    /**
     * Get the bucket store sequence id for the bucket. The sequence id is a guaranteed unique within the
     * manager id that can be used to identify the bucket. Older buckets and buckets that have not
     * had a store created for them yet will return 
     * 
     * Will be {@link BucketStore#UNASSIGNED_SEQUENCE_ID} if no bucket store has been created 
     * for the bucket or if the bucket store is an older bucket store that was not assigned a 
     * sequence id.
     * 
     * @return the sequence id for the bucket store
     */
    long getBucketStoreSequenceId();

    /**
     * Get the first id from the bucket.  Returns null if there are no entries in the bucket
     * or if the bucket has been deleted.
     * 
     * @return the id of the first message in the bucket.
     * @throws SeqStoreDatabaseException if there is an error getting the first id from the storage layer
     * @throws IllegalStateException if the bucket has been closed
     */
    AckIdV3 getFirstId() throws SeqStoreDatabaseException, IllegalStateException;

    /**
     * Get the last id from the bucket. Returns null if there are no entries in the bucket
     * or if the bucket has been deleted.
     * 
     * @return the id of the last message in the bucket.
     * @throws SeqStoreDatabaseException if there is an error getting the last id from the storage layer
     * @throws IllegalStateException if the bucket has been closed
     */
    AckIdV3 getLastId() throws SeqStoreDatabaseException, IllegalStateException;
    
    /**
     * Returns true if the count from the bucket can be trusted to be accurate. If false
     * the real count may be above the returned entry count but will never be below.
     * 
     * @return if the count can be trusted to be accurate
     */
    boolean isCountTrusted();

    /**
     * Get an accurate count of the number of entries in the bucket. This may go to the storage
     * layer to do the count.
     * 
     * @return the accurate count of the number of entries in the bucket
     * @throws SeqStoreDatabaseException if there is an error getting the count from the storage
     * @throws IllegalStateException if the bucket is not open
     */
    long getAccurateEntryCount() throws SeqStoreDatabaseException, IllegalStateException;

    /**
     * Get the position of the first entry with id lower than or equal to ackId, or 0
     * if there is no such entry.
     * 
     * @param ackId
     * @return the position of the first entry with id lower than or equal to ackId
     * @throws SeqStoreDatabaseException
     */
    StorePosition getPosition(AckIdV3 ackId) throws SeqStoreDatabaseException;

    /**
     * Close the bucket store that backs this bucket. The bucket will remain open and the store
     * will automatically be reopened if needed. Calling this has not effect if there is a 
     * an iterator active on the bucket. It can also be safely called if the bucket has
     * been closed or deleted.
     * 
     * @throws SeqStoreDatabaseException 
     */
    void closeBucketStore() throws SeqStoreDatabaseException;
    
    /**
     * Open the bucket store that backs this bucket. If the bucket is already open nothing is done.
     * This can be used to preload the bucket before the iterator reaches it.
     * 
     * @throws SeqStoreDatabaseException
     * @throws IllegalStateException if the bucket is not open
     */
    void openBucketStore() throws SeqStoreDatabaseException, IllegalStateException;
}
