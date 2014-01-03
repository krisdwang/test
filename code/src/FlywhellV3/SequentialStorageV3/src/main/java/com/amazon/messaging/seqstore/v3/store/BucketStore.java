package com.amazon.messaging.seqstore.v3.store;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;

/**
 * Interface used by a bucket to store messages. Implementations of this interface
 * must be thread safe for all operations other than close. They are guaranteed
 * that close will never be called while any other method is being called
 * in another thread and that no functions will be called
 * after close() is called.
 * 
 * @author stevenso
 *
 */
public interface BucketStore {
    /**
     * The sequence id for BucketStores that have not been assigned one.
     */
    public static long UNASSIGNED_SEQUENCE_ID = -1;
    
    /**
     * Get the bucket if for the bucket
     * 
     * @return the id for the bucket
     */
    public AckIdV3 getBucketId();
             
    /**
      * Get a unique sequence id identifying the bucket store, or {@link #UNASSIGNED_SEQUENCE_ID}
      * if the bucket store is an old one that was not assigned a sequence id
      *   
      * @return the sequence id of the bucket store or {@link #UNASSIGNED_SEQUENCE_ID} if it does not have one
      */
    long getSequenceId();
 
    /**
     * Get a the storage type used to store the bucket
     * @return
     */
    BucketStorageType getBucketStorageType();
    
    /**
     * Return a bucket cursor for the messages in this store.
     * 
     * @return
     */
    public BucketCursor createCursor() throws SeqStoreDatabaseException;

    /**
     * Store the key and value.
     */
    public void put(AckIdV3 key, StoredEntry<AckIdV3> value, boolean cache) throws SeqStoreDatabaseException;
    
    /**
     * Fetch the value for a given key
     */
    public StoredEntry<AckIdV3> get(AckIdV3 key)  throws SeqStoreDatabaseException;
    
    /**
     * Get the first id from the store. This operation may be slow.
     * @return
     * @throws SeqStoreDatabaseException
     */
    public AckIdV3 getFirstId() throws SeqStoreDatabaseException;
    
    /**
     * Get the last id from the store. This operation may be slow.
     */
    public AckIdV3 getLastId() throws SeqStoreDatabaseException;

    /**
     * Get the number of messages from the store. This operation
     * may be slow.
     */
    public long getCount() throws SeqStoreDatabaseException;

    /**
     * Get the position of the first entry with id lower than or equal to ackId
     * 
     * @return the position of the first entry with id lower than or equal to ackId
     * @throws SeqStoreDatabaseException
     */
    public StorePosition getPosition(AckIdV3 fullAckId) throws SeqStoreDatabaseException;
}
