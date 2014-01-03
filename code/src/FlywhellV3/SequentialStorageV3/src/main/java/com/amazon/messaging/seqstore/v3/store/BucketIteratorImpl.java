package com.amazon.messaging.seqstore.v3.store;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import lombok.Getter;
import lombok.Synchronized;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;

public final class BucketIteratorImpl implements BucketIterator {
    private static final Log log = LogFactory.getLog(BucketIteratorImpl.class);
    
    /**
     * Swap to using {@link BucketCursor#jumpToKey(AckIdV3)} from {@link BucketCursor#advanceToKey(AckIdV3)} 
     * if the time to advance is more than this number of milliseconds. This is intended so {@link #getPosition()} is
     * cheap for stores and readers where we're constantly moving by a small amount (e.g. stores where the dequeues
     * are keeping up with the enqueues) and {@link #advanceTo(AckIdV3)} to is cheap for cases where large jumps occur and
     * {@link #getPosition()} isn't called very often (e.g. backup stores) 
     * <p>
     * TODO: Use a better heuristic than just time for deciding between jumpToKey and advanceToKey.
     */
    private static final long ADVANCE_TO_CUTOFF_TIME = 1000;
    
    // Used to give each iterator a unique name for debugging purposes.
    private static final AtomicInteger iteratorsCreated = new AtomicInteger();
    
    private final String name;
    
    @Getter
    private final AckIdV3 bucketId;
    
    @GuardedBy("$lock")
    private final BucketCursor bucketCursor;
    
    @GuardedBy("$lock")
    private AckIdV3 knownPosition;

    @GuardedBy("$lock")
    private boolean closed = false;
    
    @GuardedBy("$lock")
    private boolean bucketDeleted = false;

    BucketIteratorImpl(AckIdV3 bucketId, BucketCursor cursor) {
        this.bucketId = bucketId;
        this.bucketCursor = cursor;
        this.name = bucketId + "::" + iteratorsCreated.incrementAndGet();
    }

    /**
     * Internal only. Used when copying an iterator.
     * 
     * @param pos position to copy
     * @throws SeqStoreDatabaseException if the cursor couldn't be copied
     */
    private BucketIteratorImpl(BucketIteratorImpl source) throws SeqStoreDatabaseException {
        source.checkNotClosed();
        bucketId = source.bucketId;
        bucketCursor = source.bucketCursor.copy();
        knownPosition = source.knownPosition;
        bucketDeleted = source.bucketDeleted;
        this.name = bucketId + "::" + iteratorsCreated.incrementAndGet();
    }
    
    @Synchronized
    BucketIteratorImpl copy() throws SeqStoreDatabaseException {
        return new BucketIteratorImpl( this );
    }

    @Override
    @Synchronized
    public AckIdV3 advanceTo(AckIdV3 id) throws SeqStoreDatabaseException {
        checkNotClosed();
        if( bucketDeleted ) return knownPosition;

        AckIdV3 start = bucketCursor.current();
        if (start != null && id.compareTo( start ) <= 0 ) {
            // Already at or above the given ack id
            return start;
        }

        if( id.getBucketPosition() != null ) {
            boolean foundExact = bucketCursor.jumpToKey( id );
            long count;
            
            if( foundExact ) {
                if( id.isInclusive() == null || id.isInclusive() == true ) {
                    count = id.getBucketPosition();
                } else {
                    count = id.getBucketPosition() - 1;
                }
                
                AckIdV3 current = bucketCursor.current();
                // current can be null if the id.coreAckId() was the first record in the bucket
                //  and id.isInclusive was false
                if( current != null ) { 
                    if( count == 0 ) {
                        log.warn( "Got a bucket position of zero but still found a record before " + id );
                    } else {
                        knownPosition = new AckIdV3( current, count );
                    }
                } else {
                    if( count != 0 ) {
                        log.warn( "Got a non-zero bucket position but ended up before any records for " + id );
                    }
                    assert knownPosition == null;
                }
            } else {
                log.warn( "Exact key " + id + " not found even though position was specified.");
            }
        } else {
            long timeDiff;
            long base;
            if( knownPosition == null ) {
                timeDiff = id.getTime() - bucketId.getTime();
                base = 0;
            } else {
                timeDiff = id.getTime() - knownPosition.getTime();
                base = knownPosition.getBucketPosition();
            }
            
            if( timeDiff <= ADVANCE_TO_CUTOFF_TIME ) {
                // For advances of less than 1 seconds worth of messages keep the count accurate to avoid
                //  seeking across a lot of messages when metrics are needed.
                long count = bucketCursor.advanceToKey(id);
                if( count > 0 ) {
                    knownPosition = new AckIdV3( bucketCursor.current(), base + count );
                }
            } else {
                bucketCursor.jumpToKey(id);
            }
        }
        
        return currentKey();
    }
    
    @Override
    @Synchronized
    public AckIdV3 advanceTo(StorePosition position) throws SeqStoreDatabaseException, IllegalArgumentException {
        checkNotClosed();
        
        if( !position.getBucketId().equals( bucketId ) ) {
            throw new IllegalArgumentException( 
                    "Tried to advance to position " + position + " on " + name + " which is for bucket " + bucketId +
                    " while position is for bucket " + position.getBucketId() );
        }
            
        if( bucketDeleted ) return knownPosition;
        
        
        AckIdV3 key = position.getKey();
        AckIdV3 start = bucketCursor.current();
        long keyComparison;
        
        if( start == null ) {
            if( key == null ) keyComparison = 0;
            else keyComparison = 1;
        } else if( key == null ) {
            keyComparison = -1;
        } else {
            keyComparison = key.compareTo( start );
        }
        
        if( keyComparison <= 0 ) {
            // Already at or above the given position
            
            // Check to see if we can bump up knownPosition
            if( key != null && ( knownPosition == null || knownPosition.compareTo( key ) < 0 ) ) {
                knownPosition = key;
            }
            
            return start;
        }
        
        if( !bucketCursor.jumpToKey(key) ) {
            bucketCursor.jumpToKey( start );
            throw new IllegalArgumentException(
                    "Tried to advance to position " + position + " on " + name + " but the bucket does not contain " +
                    "the id " + key + " that is specified by the position.");
        }
        
        knownPosition = key;
        
        return knownPosition;
    }
    
    
    /**
     * Set if entries should be evicted from the cache after they have been read
     */
    @Override
    public void setEvictFromCacheAfterReading(boolean evict) {
        bucketCursor.setEvictFromCacheAfterReading(evict);
    }
    
    /**
     * Return if entries should be evicted from the cache after they have been read
     */
    @Override
    public boolean isEvictFromCacheAfterReading() {
        return bucketCursor.isEvictFromCacheAfterReading();
    }

    @Override
    @Synchronized
    public StoredEntry<AckIdV3> next() throws SeqStoreDatabaseException {
        checkNotClosed();
        if( bucketDeleted ) return null;
        
        AckIdV3 prev = bucketCursor.current();
        StoredEntry<AckIdV3> next = bucketCursor.nextEntry(null);
        if( next == null ) return null;
        
        if( ( knownPosition != null && knownPosition.equals( prev ) ) || prev == null ) {
            if( knownPosition != null ) {
                knownPosition = new AckIdV3( next.getAckId(), knownPosition.getBucketPosition() + 1 );
            } else {
                knownPosition = new AckIdV3( next.getAckId(), 1 );
            }
            return new StoredEntryV3( knownPosition, next );
        } else {
            return next;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Attempt to use a closed bucket iterator");
        }
    }
    
    @Override
    @Synchronized
    public StorePosition getPosition() throws SeqStoreDatabaseException {
        checkNotClosed();
        
        if( bucketDeleted ) {
            // This isn't accurate but the exact position within a deleted bucket doesn't really matter.
            return new StorePosition( bucketId, knownPosition );
        }
        
        long distance = bucketCursor.getDistance( knownPosition );
        if( distance != 0 ) {
            if( knownPosition != null ) {
                knownPosition = new AckIdV3( bucketCursor.current(), knownPosition.getBucketPosition() + distance );
            } else {
                knownPosition = new AckIdV3( bucketCursor.current(), distance + 1 );
            }
        } else if( knownPosition == null && bucketCursor.current() != null ) {
            knownPosition = new AckIdV3( bucketCursor.current(), 1 );
        }
            
        return new StorePosition( bucketId, knownPosition );
    }

    @Synchronized
    void close() throws SeqStoreDatabaseException {
        if (!closed) {
            closed = true;
            bucketCursor.close();
        }
    }
    
    /**
     * Called when a bucket is about to be deleted.
     * 
     * @throws SeqStoreDatabaseException if the cursor couldn't be closed
     * 
     */
    @Synchronized
    void bucketDeleted() throws SeqStoreDatabaseException {
        bucketDeleted = true;
        bucketCursor.close();
    }

    
    @Override
    public String toString() {
        try {
            return "{" + name + "=" + getPosition() + "}";
        } catch (SeqStoreDatabaseException e) {
            return "{" + name + "= {Exception" + e.getMessage() + "} }";
        }
    }
    
    @Override
    @Synchronized
    public AckIdV3 currentKey() {
        AckIdV3 current = bucketCursor.current();
        if( current == null ) return null;
        
        // knownPosition will have bucket position information current doesn't have
        if( current.equals( knownPosition ) ) return knownPosition; 
        else return current;
    }

}
