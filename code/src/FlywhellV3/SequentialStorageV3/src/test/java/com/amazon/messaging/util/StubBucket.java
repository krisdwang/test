package com.amazon.messaging.util;

import lombok.Getter;
import lombok.Setter;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.Bucket;
import com.amazon.messaging.seqstore.v3.store.BucketIterator;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.seqstore.v3.store.StorePosition;


public class StubBucket implements Bucket {
    @Getter
    private final StoreId storeName;
    
    @Getter
    private final AckIdV3 bucketId;
    
    @Getter @Setter
    private volatile long entryCount;
    
    @Setter
    private volatile long accurateEntryCount = -1;
    
    @Getter @Setter
    private volatile long byteCount;

    @Getter @Setter
    private volatile long bucketStoreSequenceId;
    
    @Getter @Setter
    private volatile BucketStorageType bucketStorageType = BucketStorageType.SharedDatabase;

    @Getter @Setter
    private volatile AckIdV3 firstId;
    
    @Getter @Setter
    private volatile AckIdV3 lastId;
    
    @Getter @Setter
    private volatile boolean countTrusted;
    
    @Getter
    private BucketState bucketState = BucketState.OPEN;
    
    public StubBucket(StoreId storeId, AckIdV3 bucketId) {
        this.storeName = storeId;
        this.bucketId = bucketId;
        this.bucketStoreSequenceId = BucketStore.UNASSIGNED_SEQUENCE_ID;
    }
    
    public StubBucket(StoreId storeId, BucketPersistentMetaData metadata) {
        this.storeName = storeId;
        this.bucketId = metadata.getBucketId();
        this.bucketStoreSequenceId = metadata.getBucketSequenceId();
        this.entryCount = metadata.getEntryCount();
        this.byteCount = metadata.getByteCount();
        this.bucketStorageType = metadata.getBucketStorageType();
        if( metadata.isDeleted() ) {
            bucketState = BucketState.DELETED;
        }
    }
    
    @Override
    public void close() throws SeqStoreDatabaseException {
        bucketState = BucketState.CLOSED;
    }

    @Override
    public void deleted() throws SeqStoreDatabaseException {
        bucketState = BucketState.DELETED;
    }

    @Override
    public BucketPersistentMetaData getMetadata() {
        return new BucketPersistentMetaData(
                bucketStoreSequenceId, bucketId, bucketStorageType, entryCount, byteCount, bucketState == BucketState.DELETED );
    }
    
    @Override
    public long getAccurateEntryCount() throws SeqStoreDatabaseException, IllegalStateException {
        if( accurateEntryCount != -1 ) {
            entryCount = accurateEntryCount;
            accurateEntryCount = -1;
        }
        return entryCount;
    }

    @Override
    public StoredEntry<AckIdV3> get(AckIdV3 key) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(AckIdV3 key, StoredEntry<AckIdV3> value, boolean cache) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();        
    }

    @Override
    public BucketIterator getIter() throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketIterator copyIter(BucketIterator iter)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeIterator(BucketIterator iter) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumIterators() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorePosition getPosition(AckIdV3 ackId) throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeBucketStore() throws SeqStoreDatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void openBucketStore() throws SeqStoreDatabaseException, IllegalStateException {
        throw new UnsupportedOperationException();
    }
    
    
}
