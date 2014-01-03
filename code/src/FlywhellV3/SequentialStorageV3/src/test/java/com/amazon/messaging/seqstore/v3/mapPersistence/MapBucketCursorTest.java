package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.messaging.seqstore.v3.BucketCursorTestBase;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.store.BucketStore;


public class MapBucketCursorTest extends BucketCursorTestBase {
    private static final AtomicInteger bucketIdGenerator = new AtomicInteger();
    
    @Override
    public BucketStore getBucketStore(String name) throws SeqStoreException {
        ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> map = new ConcurrentSkipListMap<AckIdV3, StoredEntry<AckIdV3>>();
        AckIdV3 bucketId = new AckIdV3(5, true);
        
        return new MapBucketStore( bucketId, bucketIdGenerator.getAndIncrement(), map );
    }
    
    @Override
    public void closeBucketStore(BucketStore store) throws SeqStoreDatabaseException {
        // Nothing to do
    }
}
