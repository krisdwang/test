package com.amazon.messaging.seqstore.v3.store;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreAlreadyCreatedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDeleteInProgressException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

public interface StorePersistenceManager {
    public boolean isHealthy();
    
    /**
     * Prepare for close. Calling this is not required but may 
     * speed up shutdown by allowing long running tasks (e.g. BDB cleanup) to exit 
     * while the rest of the shutdown is running instead of waiting 
     * for the call to close before starting their shutdown.
     */
    public void prepareForClose();
    
    /**
     * Completely close and invalidate this Manager.
     * 
     * @throws SeqStoreException
     */
    public void close() throws SeqStoreException;
    
    /**
     * Record that a store is being deleted. The actual deletion
     * can happen in the background. Once the deletion is completed {@link #storeDeletionCompleted(StoreId)}
     * should be called. Stores for which {@link #storeDeletionStarted(StoreId)} 
     * has been called but haven't had {@link #storeDeletionCompleted(StoreId)} called are available
     * using {@link #getStoreIdsBeingDeleted()}. They do not appear in the set returned by 
     * {@link #getStoreIds()}.
     * 
     * @param storeName
     * @return true if the store existed and was not already marked as being deleted 
     * @throws SeqStoreDatabaseException
     */
    public boolean storeDeletionStarted(StoreId storeName) throws SeqStoreDatabaseException;
    
    /**
     * Record that a store has been fully deleted. This must be called only after 
     * {@link #storeDeletionStarted(StoreId)} has been called. If {@link #storeDeletionStarted(StoreId)}
     * has been called but the store has since been recreated calling this will do 
     * nothing.
     * <p>
     * Note that while this does check if buckets exist it may miss buckets created while it is
     * running.
     * 
     * @param storeName
     * @throws SeqStoreDatabaseException
     * @throws IllegalStateException if buckets are found for the store 
     */
    public void storeDeletionCompleted(StoreId storeName) throws SeqStoreDatabaseException;
    
    /**
     * Delete a store that has no buckets. This combines {@link #storeDeletionStarted(StoreId)}
     * and {@link #storeDeletionCompleted(StoreId)} into a single transaction.
     * <p>
     * Note that while this does check if buckets exist it may miss buckets created while it is
     * running.
     * 
     * @param storeName
     * @throws SeqStoreDatabaseException
     * @throws IllegalStateException if buckets are found for the store 
     */
    public void deleteStoreWithNoBuckets(StoreId storeName) throws SeqStoreDatabaseException; 
    
    /**
     * Return the set  of all store ids that are being deleted. These are all stores
     * for which {@link #storeDeletionStarted(StoreId)} has been called but
     * {@link #storeDeletionCompleted(StoreId)} has not been called.
     * 
     * @return the set of all store ids that are being deleted
     * @throws SeqStoreDatabaseException 
     */
    public Set<StoreId> getStoreIdsBeingDeleted() throws SeqStoreDatabaseException;
    
    /**
     * Get the buckets for the given store. 
     * @param destinationId the id of the store
     * @return The buckets for the store
     * @throws SeqStoreDatabaseException
     */
    public Collection<BucketPersistentMetaData> getBucketMetadataForStore(
            StoreId destinationId) throws SeqStoreDatabaseException;

    /**
     * Updates the metadata for the given bucket if it exists. If the bucket
     * does not exist or has been marked as pending deletion then nothing is changed
     *  
     * @param destinationId the store that contains the bucket
     * @param metadata the new metadata for the bucket
     * @return true if the metadata was updated, false if the bucket did not exist or is pending deletion
     * @throws SeqStoreDatabaseException
     */
    public boolean updateBucketMetadata( 
        StoreId destinationId, BucketPersistentMetaData metadata)
            throws SeqStoreDatabaseException;
    
    /**
     * Create a bucket store for the specified bucket.
     * 
     * @param destinationId the id of the store that this bucket is to be used for
     * @param bucketId the id of the bucket
     * @param preferredStorageType the preferred storage type for the bucket.
     * @throws IllegalStateException if the bucket already exists
     */
    public BucketStore createBucketStore( 
        StoreId destinationId, AckIdV3 bucketId, BucketStorageType preferredStorageType )
            throws SeqStoreDatabaseException, IllegalStateException;
    
    /**
     * Get a bucket store for the specified bucket. 
     * 
     * @param destinationId the id of the store that this bucket is to be used for
     * @param bucketId the id of the bucket
     * @throws IllegalStateException if the bucket doesn't exist
     */
    public BucketStore getBucketStore( StoreId destinationId, AckIdV3 bucketId )
            throws SeqStoreDatabaseException, IllegalStateException;
    
    /**
     * Close the specified bucket store.
     * @param bucketStore
     * @throws SeqStoreDatabaseException 
     */
    public void closeBucketStore( StoreId storeId, BucketStore bucketStore ) throws SeqStoreDatabaseException;
    
    public void persistReaderLevels(StoreId storeName,
        Map<String, AckIdV3> ackLevels) throws SeqStoreDatabaseException;
    
    /**
     * Get the persisted reader levels, or null if they have never been persisted
     */
    public Map<String, AckIdV3> getReaderLevels(StoreId storeId) throws SeqStoreDatabaseException;

    public void printDBStats() throws SeqStoreDatabaseException;

    /**
     * Return an MBean that can be used to get statistics about the manager. 
     * @return an MBeans for statistics about this manager, or null if this manager 
     *    does not provide an MBean
     */
    public Object getStatisticsMBean();
    

    /**
     * Return the set  of all store ids. This set is backed by the manager and will change to reflect new
     * stores being added.
     * 
     * @return the set of all store ids
     * @throws SeqStoreDatabaseException 
     */
    public Set<StoreId> getStoreIds() throws SeqStoreDatabaseException;

    /**
     * Get all the stores that are for the given group. The returned set is a snapshot
     * of the stores that exist at the time.
     * 
     * @return all stores for the group
     * @throws SeqStoreDatabaseException
     */
    public Set<StoreId> getStoreIdsForGroup(String group) throws SeqStoreDatabaseException;
    
    /**
     * Get the set of all groups for the manager. This set is a snapshot of the groups
     * and will not change.
     */
    public Set<String> getGroups();
    
    /**
     * Return true if the given store exists, else false.
     * 
     * @param store the store to check
     * @return true if store exists, else false
     * @throws SeqStoreDatabaseException
     */
    public boolean containsStore(StoreId store) throws SeqStoreDatabaseException;
    
    /**
     * Report some performance metrics about how the persistence manager is doing. Some of these metrics
     * are reported relative to the last call to this function.
     * @param metrics
     * @throws SeqStoreDatabaseException 
     */
    public void reportPerformanceMetrics(Metrics metrics) throws SeqStoreDatabaseException;

    /**
     * Get the config for this manager.
     * 
     * @return the config for this manager
     */
    public SeqStoreImmutablePersistenceConfig getConfig();

    /**
     * Returns the number of buckets that are open for reading or writing.
     */
    public int getNumOpenBuckets();
    
    /**
     * Returns the number of dedicated buckets that are open for reading or writing.
     */
    public int getNumOpenDedicatedBuckets();
    
    /**
     * Returns the number of buckets that exist. Note that this function may be expensive.
     * @throws SeqStoreDatabaseException 
     */
    public int getNumBuckets() throws SeqStoreDatabaseException;

    /**
     * Return the number of dedicated buckets that are backed by a dedicated database. Note that this function
     * may be expensive.
     */
    public int getNumDedicatedBuckets() throws SeqStoreDatabaseException;

    /**
     * Delete the bucket store.
     * 
     * @param storeId the store id of the bucket to delete
     * @param bucketId the bucket id of the bucket to delete
     * @return true if the bucket existed and was deleted
     * @throws IllegalStateException if the bucket store is still open
     */
    public boolean deleteBucketStore(StoreId storeId, AckIdV3 bucketId)
            throws SeqStoreDatabaseException, IllegalStateException;

    /**
     * Mark all the buckets for the given store as being ready for deletion. 
     * 
     * @param storeId the store id containing the buckets
     * @return a snapshot of the buckets for the store
     * @throws SeqStoreDatabaseException if there is a database error
     * @throws IllegalStateException if any of the buckets are open
     */
    public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(StoreId storeId)
        throws SeqStoreDatabaseException, IllegalStateException;

    /**
     * Returns if the give store has any buckets remaining
     */
    public boolean hasBuckets(StoreId storeId) 
            throws SeqStoreDatabaseException;

    /**
     * Create a new store. If the store is marked as having a delete in progress that is cancelled. If not 
     * all buckets from the previous instance of the store are marked as deleted this call will fail
     * with SeqStoreDeleteInProgressException
     * 
     * @param storeId
     * @param initialAckLevels
     * @throws SeqStoreDatabaseException
     * @throws SeqStoreAlreadyCreatedException if the store already exists
     * @throws SeqStoreDeleteInProgressException if buckets from the previous store with the same
     *   name are still in the process of being marked as deleted. 
     */
    public void createStore(StoreId storeId, Map<String, AckIdV3> initialAckLevels)
            throws SeqStoreDatabaseException, SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException;
    
    /**
     * Get the set of supported bucket types for new buckets. Older buckets may have other types
     * but no new buckets should be created with those types
     * 
     * @return the set of supported bucket types for new buckets
     */
    public Set<BucketStorageType> getSupportedNewBucketTypes();
}