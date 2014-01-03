package com.amazon.messaging.seqstore.v3.internal;

import com.amazon.messaging.seqstore.v3.SeqStoreReaderMetrics;
import com.amazon.messaging.seqstore.v3.StoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.store.Store;
import com.amazon.messaging.seqstore.v3.store.StoreIterator;
import com.amazon.messaging.seqstore.v3.store.StorePosition;

class StoreReaderImpl implements StoreReader {

    private final StoreIterator dequeueItr;

    private final Store store_;

    private volatile StoredEntry<AckIdV3> peekedEntry = null;

    StoreReaderImpl(Store store, AckIdV3 startingId) throws SeqStoreDatabaseException, SeqStoreClosedException {
        store_ = store;
        dequeueItr = store_.getIterAt(startingId);
    }

    @Override
    public synchronized StoredEntry<AckIdV3> dequeue() throws SeqStoreException {
        if (peekedEntry != null) {
            StoredEntry<AckIdV3> next = peekedEntry;
            peekedEntry = null;
            return next;
        }

        StoredEntry<AckIdV3> result = dequeueItr.next();
        return result;
    }

    @Override
    public synchronized StoredEntry<AckIdV3> peek() throws SeqStoreException {
        if (peekedEntry != null) {
            return peekedEntry;
        }
        peekedEntry = dequeueItr.next();
        return peekedEntry;
    }

    @Override
    public SeqStoreReaderMetrics getStoreBacklogMetrics(AckIdV3 ackLevel) throws SeqStoreException {
        StorePosition dequeuePos;
        synchronized (this) {
            dequeuePos = dequeueItr.getPosition();
        }
        return store_.getStoreBacklogMetrics( ackLevel,  dequeuePos );
    }

    @Override
    public synchronized void close() {
        dequeueItr.close();
    }

    @Override
    public synchronized AckIdV3 getPosition() {
        if(peekedEntry != null)
            return new AckIdV3(peekedEntry.getAckId(), false);

        return dequeueItr.getCurKey();
    }

    @Override
    public synchronized AckIdV3 advanceTo(AckIdV3 pos) throws SeqStoreDatabaseException, SeqStoreClosedException {
        if( peekedEntry != null && peekedEntry.getAckId().compareTo( pos ) < 0 ) peekedEntry = null;
        return dequeueItr.advanceTo(pos);
    }
    
    @Override
    public synchronized void setEvictFromCacheAfterReading(boolean evict) {
        dequeueItr.setEvictFromCacheAfterReading(evict);
    }
    
    @Override
    public synchronized boolean isEvictFromCacheAfterReading() {
        return dequeueItr.isEvictFromCacheAfterReading();
    }
    
}
