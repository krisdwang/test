package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import lombok.Getter;
import lombok.Setter;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.store.BucketCursor;

public final class MapBucketCursor implements BucketCursor {
    private final ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> bucket;
    
    private AckIdV3 current;
    
    private boolean closed = false;
    
    // Doesn't actually do anything for this implementation
    @Getter @Setter
    private boolean evictFromCacheAfterReading;
    
    MapBucketCursor(ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> bucket ) {
        this.bucket = bucket;
    }
    
    MapBucketCursor(MapBucketCursor source ) {
        bucket = source.bucket;
        current = source.current;
    }
    
    @Override
    public BucketCursor copy() throws SeqStoreDatabaseException {
        checkNotClosed();
        
        return new MapBucketCursor( this );
    }

    @Override
    public void close() throws SeqStoreDatabaseException {
        closed = true;
    }

    @Override
    public AckIdV3 current() {
        return current;
    }
    
    @Override
    public StoredEntry<AckIdV3> nextEntry(AckIdV3 max)
        throws SeqStoreDatabaseException
    {
        checkNotClosed();
        
        Map.Entry<AckIdV3, StoredEntry<AckIdV3>> next;
        if( current == null ) {
            next = bucket.firstEntry();
        } else {
            next = bucket.higherEntry( current );
        }
        
        if( next == null ) return null;
        
        if (max != null && next.getKey().compareTo(max) > 0) {
            return null;
        }
        
        current = next.getKey();
        return next.getValue();
    }

    @Override
    public long advanceToKey(AckIdV3 key) throws SeqStoreDatabaseException {
        checkNotClosed();
        
        long count = 0;
        while( nextEntry( key ) != null ) count++;
        
        return count;
    }
    
    @Override
    public boolean jumpToKey(AckIdV3 key) throws SeqStoreDatabaseException {
        current = bucket.floorKey(key);
        return bucket.containsKey( key.getCoreAckId() );
    }
    
    private void checkNotClosed() {
        if( closed ) throw new IllegalStateException( "Attempt to use closed cursor.");
    }
    
    @Override
    public long getDistance(AckIdV3 key) throws SeqStoreDatabaseException {
        if( key == current || ( key != null && key.equals( current ) ) ) return 0;
        
        if( key == null ) key = AckIdV3.MINIMUM;
        
        long size;
        if( current != null ) { 
            AckIdV3 min = AckIdV3.min( key, current );
            AckIdV3 max = AckIdV3.max( key, current );
            size = bucket.subMap(min, false, max, false).size();
        } else {
            size = bucket.headMap(key, false).size();
        }
        if( bucket.containsKey( key ) ) ++size;
        return size;
    }
}
