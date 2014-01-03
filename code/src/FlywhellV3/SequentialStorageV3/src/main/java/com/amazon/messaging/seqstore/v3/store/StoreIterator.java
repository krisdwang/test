package com.amazon.messaging.seqstore.v3.store;

import net.jcip.annotations.NotThreadSafe;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

/**
 * An iterator that is limited to not advancing beyond the last available entry for a store. This
 * class is not thread safe and needs to be externally synchronized.
 *
 * @param <E>
 * @param <BucketType>
 */
@NotThreadSafe
public final class StoreIterator implements BucketJumperInterface {

    private final Store store_;
    private final BucketJumperInterface jumper_;
    private boolean closed_;

    // start at the beginning
    StoreIterator(BucketManager bm, Store store) {
        jumper_ = new BucketJumper( bm );
        store_ = store;
        closed_ = false;
    }

    // start at the ackId
    StoreIterator(BucketManager bm, AckIdV3 ackIdToStart, Store store) throws SeqStoreDatabaseException, SeqStoreClosedException {
        this(bm, store);
        closed_ = false;
        advanceTo(ackIdToStart);
    }
    
    // copy
    StoreIterator(StoreIterator copy) throws SeqStoreException {
        store_ = copy.store_;
        jumper_ = copy.jumper_.copy();
        closed_ = false;
    }

    @Override
    public StoredEntry<AckIdV3> next() throws SeqStoreException {
        checkClosed();
        
        AckIdV3 lastAvailableId = store_.getLastAvailableId();

        StoredEntry<AckIdV3> result = null;
        // if there are things available and curKey is less than lastAvailable,
        // get next
        if ((lastAvailableId != null) && ((getCurKey() == null) || (getCurKey().compareTo(lastAvailableId) < 0))) {
            result = jumper_.next();
        } else {
            // the curKey has to point to lastAvailable now
            assert ((lastAvailableId == null) || (getCurKey().compareTo(lastAvailableId) == 0)) 
                : "CurKey=" + getCurKey() + " LastAvailableId=" + lastAvailableId; 

            // try to update last available
            store_.updateAvailable();
            lastAvailableId = store_.getLastAvailableId();
            // if there are things available and curKye is less than
            // lastAvailable, get next
            if ((lastAvailableId != null) && ((getCurKey() == null) || (getCurKey().compareTo(lastAvailableId) < 0))) {
                result = jumper_.next();
            }
        }
        
        if( result != null ) assertDidntGoBeyondLastAvailable(result.getAckId());

        return result;
    }

    private void assertDidntGoBeyondLastAvailable(AckIdV3 result) {
        AckIdV3 lastAvailableId = store_.getLastAvailableId();
        assert result == null || lastAvailableId == null || result.compareTo( lastAvailableId ) <= 0 : 
            "Skipped ahead of last available message " + lastAvailableId + " to message " + result; 
    }

    @Override
    public AckIdV3 advanceTo(AckIdV3 key) throws SeqStoreDatabaseException, SeqStoreClosedException {
        checkClosed();
        
        AckIdV3 result = null;
        AckIdV3 lastAvailableId = store_.getLastAvailableId();

        if ((lastAvailableId == null) || (key.compareTo(lastAvailableId) > 0)) {
            store_.updateAvailable();
            lastAvailableId = store_.getLastAvailableId();
            if (lastAvailableId != null) {
                result = jumper_.advanceTo(AckIdV3.min(lastAvailableId, key));
                assertDidntGoBeyondLastAvailable(result);
            }
        } else {
            result = jumper_.advanceTo(key);
            assertDidntGoBeyondLastAvailable(result);
        }
        return result;
    }

    @Override
    public AckIdV3 getCurKey() {
        checkClosed();
        
        return jumper_.getCurKey();
    }

    @Override
    public void close() {
        checkClosed();
        
        jumper_.close();
    }

    @Override
    public BucketJumperInterface copy() throws SeqStoreException {
        checkClosed();
        
        return new StoreIterator(this);
    }

    @Override
    public StorePosition getPosition() throws SeqStoreDatabaseException {
        checkClosed();
        
        return jumper_.getPosition();
    }
    
    @Override
    public void setEvictFromCacheAfterReading(boolean evict) {
        jumper_.setEvictFromCacheAfterReading(evict);
    }
    
    @Override
    public boolean isEvictFromCacheAfterReading() {
        return jumper_.isEvictFromCacheAfterReading();
    }
    
    private void checkClosed() {
        if( closed_ ) throw new IllegalStateException( "Attempt to act on a closed StoreIterator.");
    }
}
