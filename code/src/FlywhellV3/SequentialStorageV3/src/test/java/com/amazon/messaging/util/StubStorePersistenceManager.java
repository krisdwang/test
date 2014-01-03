package com.amazon.messaging.util;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import lombok.Setter;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreAlreadyCreatedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDeleteInProgressException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;

public class StubStorePersistenceManager implements StorePersistenceManager {
    @Setter
    private boolean health = true;
    
    private boolean closed = false;
    
    @Setter
    private SeqStoreImmutablePersistenceConfig config;
    
    public StubStorePersistenceManager() {
        SeqStorePersistenceConfig tmpConfig = new SeqStorePersistenceConfig();
        tmpConfig.setStoreDirectory(new File("/tmp/"));
        tmpConfig.setNumDedicatedDBDeletionThreads(1);
        tmpConfig.setNumSharedDBDeletionThreads(1);
        tmpConfig.setNumStoreDeletionThreads(1);
        config = tmpConfig.getImmutableConfig();
    }

    public StubStorePersistenceManager(SeqStoreImmutablePersistenceConfig config) {
        this.config = config;
    }

    @Override
    public boolean isHealthy() {
        return !closed && health;
    }

    @Override
    public void prepareForClose() {
    }
    
    @Override
    public void close() throws SeqStoreException {
        closed = true;
    }
    
    @Override
    public SeqStoreImmutablePersistenceConfig getConfig() {
        return config;
    }

    @Override
    public boolean storeDeletionStarted(StoreId storeName) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void storeDeletionCompleted(StoreId storeName) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();        
    }

    @Override
    public void deleteStoreWithNoBuckets(StoreId storeName) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();        
    }

    @Override
    public Set<StoreId> getStoreIdsBeingDeleted() throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<BucketPersistentMetaData> getBucketMetadataForStore(StoreId destinationId)
        throws SeqStoreDatabaseException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean updateBucketMetadata(StoreId destinationId, BucketPersistentMetaData metadata)
        throws SeqStoreDatabaseException
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public BucketStore createBucketStore(
        StoreId destinationId, AckIdV3 bucketId, BucketStorageType preferredStorageType)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketStore getBucketStore(
        StoreId destinationId, AckIdV3 bucketId)
        throws SeqStoreDatabaseException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeBucketStore(StoreId storeId, BucketStore bucketStore) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();        
    }

    @Override
    public void persistReaderLevels(StoreId storeName, Map<String, AckIdV3> ackLevels)
        throws SeqStoreDatabaseException
    {
        throw new UnsupportedOperationException();        
    }

    @Override
    public Map<String, AckIdV3> getReaderLevels(StoreId storeId) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void printDBStats() throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();        
    }

    @Override
    public Object getStatisticsMBean() {
        throw new UnsupportedOperationException();
    }
    

    @Override
    public Set<StoreId> getStoreIds() throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<StoreId> getStoreIdsForGroup(String group) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getGroups() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsStore(StoreId store) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reportPerformanceMetrics(Metrics metrics) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();        
    }

    @Override
    public int getNumOpenBuckets() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumOpenDedicatedBuckets() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumBuckets() throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumDedicatedBuckets() throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteBucketStore(StoreId storeId, AckIdV3 bucketId)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(StoreId storeId)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasBuckets(StoreId storeId) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createStore(StoreId storeId, Map<String, AckIdV3> initialAckLevels)
        throws SeqStoreDatabaseException, SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException
    {
        throw new UnsupportedOperationException();        
    }
    
    @Override
    public Set<BucketStorageType> getSupportedNewBucketTypes() {
        throw new UnsupportedOperationException();
    }
}
