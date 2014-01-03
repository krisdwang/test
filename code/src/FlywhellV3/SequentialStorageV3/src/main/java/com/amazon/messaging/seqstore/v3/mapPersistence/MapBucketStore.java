package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.BucketCursor;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.seqstore.v3.store.StorePosition;

public class MapBucketStore implements BucketStore {
    private final AckIdV3 bucketId;
    
    private final long sequenceId;
    
    private final ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> bucket_;
    
    public MapBucketStore(
        AckIdV3 bucketId, long bucketSequenceId, 
        ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> bucket) 
    {
        this.bucketId = bucketId;
        this.bucket_ = bucket;
        this.sequenceId = bucketSequenceId;
    }
    
    @Override
    public long getSequenceId() {
        return sequenceId;
    }

    @Override
    public void put(AckIdV3 key, StoredEntry<AckIdV3> value, boolean cache) throws SeqStoreDatabaseException {
        bucket_.put(key, value);
    }
    
    @Override
    public StoredEntry<AckIdV3> get(AckIdV3 key) throws SeqStoreDatabaseException {
        return bucket_.get( key );
    }
    
    @Override
    public StorePosition getPosition(AckIdV3 fullAckId) throws SeqStoreDatabaseException {
        NavigableMap<AckIdV3, StoredEntry<AckIdV3>> headMap = bucket_.headMap(fullAckId, true);
        if( !headMap.isEmpty() ) {
            return new StorePosition(bucketId, new AckIdV3( headMap.lastKey(), headMap.size() ) );
        } else {
            return new StorePosition(bucketId, null );
        }
    }
    
    @Override
    public BucketCursor createCursor() throws SeqStoreDatabaseException {
        return new MapBucketCursor( bucket_ );
    }

    @Override
    public long getCount() throws SeqStoreDatabaseException {
        return bucket_.size();
    }

    @Override
    public AckIdV3 getFirstId() throws SeqStoreDatabaseException {
        Map.Entry<AckIdV3, StoredEntry<AckIdV3> > entry = bucket_.firstEntry();
        if( entry == null ) return null;
        return entry.getKey();
    }

    @Override
    public AckIdV3 getLastId() throws SeqStoreDatabaseException {
        Map.Entry<AckIdV3, StoredEntry<AckIdV3> > entry = bucket_.lastEntry();
        if( entry == null ) return null;
        return entry.getKey();
    }

    @Override
    public BucketStorageType getBucketStorageType() {
        return BucketStorageType.DedicatedDatabase;
    }

    @Override
    public AckIdV3 getBucketId() {
        return bucketId;
    }
}
