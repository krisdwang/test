package com.amazon.messaging.seqstore.v3.store;

import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.jcip.annotations.NotThreadSafe;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

/**
 * An iterator that wraps BucketIterator. However unlike BucketIerator is will
 * jump from one bucket to the next once it has iterated over all the messages
 * in a particular bucket. This abstracts the notion of multiple buckets away
 * from higher layers of the code.
 * <p>
 * This iterator is only safe for ack levels below the insertion level.
 * <p>
 * This class is not thread safe and must be externally synchronized
 * 
 * @author kaitchuc
 */
@NotThreadSafe
public final class BucketJumper implements BucketJumperInterface {
    private static final Log log = LogFactory.getLog( BucketJumper.class );
    
    private final AtomicLong bucketJumperNameCounter = new AtomicLong();

    private final BucketManager bm_;
    
    private final String name_;

    // bucket iterator for each bucket
    private BucketIterator dequeueItr_;

    private AckIdV3 curKey_;
    
    private boolean closed_;
    
    private boolean evictFromCacheAfterReading;

    /**
     * Creates a iterator that starts at this id. That is to say, that the first
     * call to next, will return the entry *after* this id.
     */
    public BucketJumper(BucketManager bm) {
        bm_ = bm;
        dequeueItr_ = null;
        curKey_ = null;
        closed_ = false;
        name_ = "BucketJumper{" + bm.getStoreId() + "," + bucketJumperNameCounter.incrementAndGet() + "}";
    }
    
    public BucketJumper(String name, BucketManager bm) {
        bm_ = bm;
        dequeueItr_ = null;
        curKey_ = null;
        closed_ = false;
        name_ = "BucketJumper{" + name + "}";
    }
    
    public BucketJumper(BucketJumper jumper) throws SeqStoreDatabaseException, SeqStoreClosedException {
        bm_ = jumper.bm_;
        if( jumper.dequeueItr_ != null ) {
            dequeueItr_ = bm_.copyBucketIterator( jumper.dequeueItr_ );
        } else {
            dequeueItr_ = null;
        }
        curKey_ = jumper.curKey_;
        closed_ = false;
        name_ = jumper.name_ + "{Copy}";
    }

    @Override
    public StoredEntry<AckIdV3> next() throws SeqStoreException {
        checkClosed();
        
        StoredEntry<AckIdV3> entry = null;
        // if there are elements still in the bucket try to get from it
        if (dequeueItr_ != null) {
            entry = dequeueItr_.next();
        }
        
        // Loop because buckets could be deleted as we go
        while( entry == null ) {
            BucketManager.GetNextBucketIteratorResult result = bm_.getNextBucketIterator( 
                    dequeueItr_ );
            
            if( result.isReachedEnd() ) {
                return null;
            }
            
            if( log.isDebugEnabled() ) {
                log.debug( name_ + ": Leaving bucket " + ( dequeueItr_ == null ? "<null>" : dequeueItr_.getBucketId() ) );
            }
            
            dequeueItr_ = result.getIterator();
            dequeueItr_.setEvictFromCacheAfterReading(evictFromCacheAfterReading);
            entry = dequeueItr_.next();
            if( log.isDebugEnabled() ) {
                log.debug( name_ + ": Moved to bucket" + dequeueItr_.getBucketId() + " got entry " + ( entry == null ? "null" : entry.getAckId() ) );
            }
        }
        
        curKey_ = entry.getAckId();
        
        return entry;
    }

    @Override
    public AckIdV3 advanceTo(AckIdV3 id) throws SeqStoreDatabaseException, SeqStoreClosedException {
        checkClosed();
        
        // never goes back
        if ((curKey_ != null) && (id.compareTo(curKey_) <= 0)) {
            return curKey_;
        }

        AckIdV3 oldBucketId = dequeueItr_ == null ? null : dequeueItr_.getBucketId();
        dequeueItr_ = bm_.getBucketIteratorForKey( id, dequeueItr_, evictFromCacheAfterReading );
        if (dequeueItr_ != null ) {
            curKey_ = dequeueItr_.currentKey();
            
            if( log.isDebugEnabled() && ( oldBucketId == null || !dequeueItr_.getBucketId().equals( oldBucketId ) ) ) {
                log.debug( name_ + ": Advanced to bucket " + dequeueItr_.getBucketId() + " from " + oldBucketId + " and reached " + curKey_ );
            }
        } else {
            // We're now at the beginning of the store
            curKey_ = null;
            
            if( log.isDebugEnabled() && oldBucketId != null ) {
                log.debug( name_ + ": Left bucket " + oldBucketId + ". It was deleted and nothing remains before " + id );
            }
        }
        
        return curKey_;
    }

    @Override
    public StorePosition getPosition() throws SeqStoreDatabaseException {
        checkClosed();
        StorePosition position;
        if (dequeueItr_ == null) {
            return null;
        }

        position = dequeueItr_.getPosition();
        return position;
    }

    @Override
    public AckIdV3 getCurKey() {
        checkClosed();
        return curKey_;
    }

    @Override
    public void close() {
        if ( !closed_ ) {
            closed_ = true;
            if( dequeueItr_ != null ) {
                try {
                    bm_.closeBucketIterator( dequeueItr_ );
                } catch (SeqStoreException e) {
                    log.warn( name_ + ": Failed closing bucket iterator.", e );
                }
                dequeueItr_ = null;
            }
        }
    }
    
    @Override
    public BucketJumper copy() throws SeqStoreDatabaseException, SeqStoreClosedException {
        checkClosed();
        return new BucketJumper( this );
    }
    
    
    private void checkClosed() {
        if( closed_ ) throw new IllegalStateException( "Attempt to act on a closed BucketJumper.");
    }
    
    @Override
    public void setEvictFromCacheAfterReading(boolean evict) {
        evictFromCacheAfterReading = evict;
        if( dequeueItr_ != null ) dequeueItr_.setEvictFromCacheAfterReading(evict);
    }
    
    @Override
    public boolean isEvictFromCacheAfterReading() {
        return evictFromCacheAfterReading;
    }
}
