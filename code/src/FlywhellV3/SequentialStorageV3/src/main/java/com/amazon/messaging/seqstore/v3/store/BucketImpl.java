package com.amazon.messaging.seqstore.v3.store;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.Getter;

import net.jcip.annotations.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.utils.AtomicComparableReferenceUtils;
import com.amazon.messaging.utils.StateManager;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public final class BucketImpl implements Bucket {
    private static final Log log = LogFactory.getLog(BucketImpl.class);
    
    // A special sequence id used to indicate that the store for the bucket has not been created yet
    private static final long STORE_NOT_CREATED_SEQUENCE_ID = -2;
    
    private static final AtomicIntegerFieldUpdater<BucketImpl> countUpdater = 
            AtomicIntegerFieldUpdater.newUpdater(BucketImpl.class, "count");
    
    private static final AtomicLongFieldUpdater<BucketImpl> byteCountUpdater = 
            AtomicLongFieldUpdater.newUpdater(BucketImpl.class, "byteCount");
    
    private static final AtomicReferenceFieldUpdater<BucketImpl, AckIdV3> firstIdUpdater = 
            AtomicReferenceFieldUpdater.newUpdater(BucketImpl.class, AckIdV3.class, "firstId");
    
    private static final AtomicReferenceFieldUpdater<BucketImpl, AckIdV3> lastIdUpdater = 
            AtomicReferenceFieldUpdater.newUpdater(BucketImpl.class, AckIdV3.class, "lastId");
    
    private static final AtomicReferenceFieldUpdater<BucketImpl, StoreAndLock> storeAndLockUpdater = 
            AtomicReferenceFieldUpdater.newUpdater(BucketImpl.class, StoreAndLock.class, "storeAndLock");
    
    private static final class StoreAndLock {
        public final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        
        @GuardedBy("storeLock")
        public BucketStore store;
        
        private final Object itersLock = new Object();
        
        @GuardedBy("itersLock") 
        private Set<BucketIteratorImpl> iters;
        
        public boolean hasIterators() {
            synchronized ( itersLock ) {
                return iters != null;
            }
        }
        
        public BucketIteratorImpl addIterator() throws SeqStoreDatabaseException {
            BucketIteratorImpl itr = new BucketIteratorImpl( store.getBucketId(), store.createCursor() );
            synchronized ( itersLock ) {
                if( iters == null ) {
                    iters = new HashSet<BucketIteratorImpl>(4);
                }
                iters.add(itr);
            }
            return itr;
        }
        
        public BucketIteratorImpl copyIterator(BucketIteratorImpl source) throws SeqStoreDatabaseException {
            BucketIteratorImpl itr = source.copy();
            synchronized ( itersLock ) {
                if( iters == null ) {
                    iters = new HashSet<BucketIteratorImpl>(4);
                }
                iters.add(itr);
            }
            return itr;
        }
        
        public void removeIterator(BucketIteratorImpl itr) {
            synchronized ( itersLock ) {
                if( iters == null ) return;
                if( iters.remove(itr) && iters.isEmpty() ) {
                    iters = null;
                }
            }
        }
        
        /**
         * Get the number of iterators for the store. This is safe to call without
         * holding the lock.
         */
        public int getNumIterators() {
            synchronized ( itersLock ) {
                if( iters == null ) return 0;
                return iters.size();
            }
        }
        
        public void closeAllIterators() throws SeqStoreDatabaseException {
            synchronized ( itersLock ) {
                if( iters != null ) {
                    for (BucketIteratorImpl iter : iters ) {
                        iter.close();
                    }
                    iters = null;
                }
            }
        }
        
        public void prepareIteratorsForDeletion() throws SeqStoreDatabaseException {
            synchronized ( itersLock ) {
                if( iters != null ) {
                    for (BucketIteratorImpl iter : iters ) {
                        iter.bucketDeleted();
                    }
                    iters = null;
                }
            }
        }
    }
    
    private final StateManager<BucketState, IllegalStateException> stateManager;
    
    // The store and lock for that store. This should only be set using storeLockUpdater. Any thread
    // changing it must own the write lock of the StoreAndLock that it is removing or adding and use
    // compare and set to ensure no other thread has changed it in parallel. Any thread setting 
    // storeAndLock must ensure that the store for the storeAndLock is not null before it releases 
    // the write lock
    private volatile StoreAndLock storeAndLock;
    
    private final StorePersistenceManager persistenceManager;
    
    private volatile int count; 
    
    private volatile long byteCount;
    
    @Getter
    private final StoreId storeName;
    
    private volatile long bucketStoreSequenceId;
    
    private volatile BucketStorageType bucketStorageType;

    @Getter
    private final AckIdV3 bucketId;
    
    private volatile AckIdV3 firstId;
    
    private volatile boolean firstIdLoaded;

    private volatile AckIdV3 lastId;
    
    private volatile boolean lastIdLoaded;
    
    @Getter
    private volatile boolean countTrusted;
    
    /**
     * Create a new bucket
     */
    public static Bucket createNewBucket(
        StoreId storeName, AckIdV3 bucketId, 
        BucketStorageType bucketStorageType, StorePersistenceManager persistenceManager)
            throws SeqStoreDatabaseException
    {
        return new BucketImpl(storeName, bucketId, bucketStorageType, persistenceManager);
    }
    
    /**
     * Restore an existing bucket.
     */
    public static Bucket restoreBucket(
        StoreId storeName, BucketPersistentMetaData metadata,
        StorePersistenceManager persistenceManager)
    {
        return new BucketImpl(storeName, metadata, persistenceManager);
    }
    
    /**
     * Constructor for restoring an existing bucket
     */
    private BucketImpl(
        StoreId storeName, BucketPersistentMetaData metadata, 
        StorePersistenceManager persistenceManager )
    {
        this.storeName = storeName;
        this.bucketId = metadata.getBucketId();
        this.persistenceManager = persistenceManager;
        this.bucketStoreSequenceId = metadata.getBucketSequenceId();
        this.bucketStorageType = metadata.getBucketStorageType();
        
        stateManager = new StateManager<BucketState, IllegalStateException>( 
                BucketState.OPEN, StateManager.<BucketState>getIllegalStateExceptionGenerator(), false );
        
        count = (int) metadata.getEntryCount();
        byteCount = metadata.getByteCount();
        firstIdLoaded = false;
        lastIdLoaded = false;
        countTrusted = false;
    }
    
    /**
     * Constructor for a new empty bucket where it is known that no data is in the bucket.
     */
    private BucketImpl(
        StoreId storeName, AckIdV3 bucketId, 
        BucketStorageType bucketStorageType, StorePersistenceManager persistenceManager )
            throws SeqStoreDatabaseException 
    {
        this.storeName = storeName;
        this.bucketId = bucketId;
        this.persistenceManager = persistenceManager;
        this.bucketStorageType = bucketStorageType;
        this.bucketStoreSequenceId = STORE_NOT_CREATED_SEQUENCE_ID;
        
        stateManager = new StateManager<BucketState, IllegalStateException>( 
                BucketState.OPEN, StateManager.<BucketState>getIllegalStateExceptionGenerator(), false );
        
        count = 0;
        byteCount = 0;
        firstIdLoaded = true;
        lastIdLoaded = true;
        countTrusted = true;
    }
    
    private boolean currentThreadHasReadLocks() {
        StoreAndLock currentStoreAndLock = storeAndLock;
        return currentStoreAndLock != null && currentStoreAndLock.lock.getReadHoldCount() != 0;
    }
    
    private void releaseReadLock() {
        StoreAndLock currentStoreAndLock = storeAndLock;
        if( currentStoreAndLock == null ) throw new IllegalMonitorStateException();
        currentStoreAndLock.lock.readLock().unlock();
    }
    
    private void releaseWriteLock() {
        StoreAndLock currentStoreAndLock = storeAndLock;
        if( currentStoreAndLock == null ) throw new IllegalMonitorStateException();
        currentStoreAndLock.lock.writeLock().unlock();
    }
    
    /**
     * Acquire an existing lock. If this returns true then the lock in storeAndLock is owned
     * for by this thread and the store in storeAndLock is loaded. If it returns false
     * there is no existing storeAndLock or it was closed and reopened while this function
     * was running.
     *  
     * @return if an existing lock could be acquired
     */
    @SuppressWarnings("UL_UNRELEASED_LOCK")
    private boolean acquireExistingLock( boolean writeLock ) {
        StoreAndLock currentStoreAndLock = storeAndLock;
        if( currentStoreAndLock == null ) {
            // No existing lock to acquire
            return false;
        }
        
        if( writeLock ) {
            currentStoreAndLock.lock.writeLock().lock();
        } else {
            currentStoreAndLock.lock.readLock().lock();
        }
        
        if( currentStoreAndLock != storeAndLock ) {
            // Some other thread changed the lock so unlock and return
            if( writeLock ) {
                currentStoreAndLock.lock.writeLock().unlock();
            } else {
                currentStoreAndLock.lock.readLock().unlock();
            }
            return false;
        } 
        
        return true;
    }
    
    /**
     * Acquire a new lock. If this returns true then this thread owns the write lock
     * for storeAndLock and is responsible for loading the store before releasing the lock
     */
    @SuppressWarnings("UL_UNRELEASED_LOCK")
    private boolean acquireNewLock() {
        if( storeAndLock != null ) {
            return false;
        }
        
        StoreAndLock newStoreAndLock = new StoreAndLock();
        newStoreAndLock.lock.writeLock().lock();
        
        boolean success = false;
        try {
            success = storeAndLockUpdater.compareAndSet(this, null, newStoreAndLock);
        } finally {
            if( !success ) {
                newStoreAndLock.lock.writeLock().unlock();
            }
        }
        
        return success;
    }
    
    /**
     * Load the BucketStore. When this function returns successfully it is guaranteed that the store
     * is loaded and the read or write lock (depending on the value of writeLock) is held
     * to ensure that the store is not unloaded while processing. On failure this function
     * ensures that all locks are released before it returns.
     *  
     * @param writeLock
     * @throws SeqStoreDatabaseException
     */
    private void loadStore(boolean writeLock) throws SeqStoreDatabaseException {
        if( currentThreadHasReadLocks() && writeLock ) {
            // Asking for the write lock while you've already got the read lock
            // will deadlock
            throw new IllegalMonitorStateException();
        }
        
        boolean acquiredExisting;
        boolean acquiredNew;
        do {
            acquiredExisting = acquireExistingLock(writeLock);
            if( !acquiredExisting ) {
                acquiredNew = acquireNewLock();
            } else {
                acquiredNew = false;
            }
        } while( !acquiredNew && !acquiredExisting );
        
        if( acquiredExisting ) return;
        
        boolean success = false;
        StoreAndLock currentStoreAndLock = storeAndLock;
        try {
            assert currentStoreAndLock != null && currentStoreAndLock.store == null;
            
            BucketStore store;
            if( bucketStoreSequenceId == STORE_NOT_CREATED_SEQUENCE_ID ) {
                store = persistenceManager.createBucketStore(storeName, bucketId, bucketStorageType);
                bucketStorageType = store.getBucketStorageType();
                bucketStoreSequenceId = store.getSequenceId();
            } else {
                store = persistenceManager.getBucketStore(storeName, bucketId);
            }
            
            currentStoreAndLock.store = store;
            
            if( !writeLock ) {
                // get a read lock before releasing the write lock
                currentStoreAndLock.lock.readLock().lock();
            }
            success = true;
        } finally {
            if( currentStoreAndLock.store == null ) {
                storeAndLock = null; // The load failed so make sure nothing tries to use it
                assert !success;
            }
            
            if( !success || !writeLock ) currentStoreAndLock.lock.writeLock().unlock();
        }
    }
    
    @Override
    public void closeBucketStore() throws SeqStoreDatabaseException {
        StoreAndLock currentStoreAndLock = storeAndLock;
        if( currentStoreAndLock == null ) return;

        // There is no need to take the stateLock as the storeLock will protect 
        //  against parallel close or delete causing problems
        currentStoreAndLock.lock.writeLock().lock();
        try {
            if( currentStoreAndLock != storeAndLock ) {
                // Another thread closed the storeAndLock while this thread
                // was waiting for the write lock
                return;
            }
            
            if( !currentStoreAndLock.hasIterators() ) {
                // Don't need to worry about iters being added after the check as adding an 
                // iterator requires a read lock on the store
                persistenceManager.closeBucketStore(storeName, currentStoreAndLock.store );
                if( !storeAndLockUpdater.compareAndSet(this, currentStoreAndLock, null) ) {
                    throw new RuntimeException( 
                            "Coding error. No thread should change storeAndLock without the write lock");
                }
            }
        } finally {
            currentStoreAndLock.lock.writeLock().unlock();
        }
    }
    
    
    @Override
    public void openBucketStore() throws SeqStoreDatabaseException {
        stateManager.lockRequiredState( BucketState.OPEN, "closeBucketStore must be called only on open buckets" );
        try {
            loadStore(false);
            releaseReadLock();
        } finally {
            stateManager.unlock();
        }
    }
    
    private void loadFirstId() throws SeqStoreDatabaseException {
        if( !firstIdLoaded ) {
            loadStore(false);
            try {
                AckIdV3 firstStoreId = storeAndLock.store.getFirstId();
                if( firstStoreId != null ) {
                    AtomicComparableReferenceUtils.decreaseTo(firstIdUpdater, this, firstStoreId);
                }
                firstIdLoaded = true;
            } finally {
                releaseReadLock();
            }
        }
    }
    
    private void loadLastId() throws SeqStoreDatabaseException {
        if( !lastIdLoaded ) {
            loadStore(false);
            try {
                AckIdV3 lastStoreId = storeAndLock.store.getLastId();
                if( lastStoreId != null ) {
                    AtomicComparableReferenceUtils.increaseTo(lastIdUpdater, this, lastStoreId);
                }
                lastIdLoaded = true;
            } finally {
                releaseReadLock();
            }
        }
    }

    @Override
    public void put(AckIdV3 key, StoredEntry<AckIdV3> value, boolean cache)
        throws SeqStoreDatabaseException 
    {
        stateManager.lockRequiredState( BucketState.OPEN, "No new inserts are allowed for a closed or deleted Bucket" );
        try {
            // Must be done before the countInProgressLock and put 
            countUpdater.incrementAndGet(this);
            byteCountUpdater.addAndGet(this, value.getPayloadSize() + AckIdV3.LENGTH);
            
            loadStore(false);
            try {
                storeAndLock.store.put( key, value, cache );
            } finally {
                releaseReadLock();
            }
            
            loadFirstId();
            AtomicComparableReferenceUtils.decreaseTo(firstIdUpdater, this, key);
            loadLastId();
            AtomicComparableReferenceUtils.increaseTo(lastIdUpdater, this, key);
        } finally {
            stateManager.unlock();
        }
    }
    
    @Override
    public StoredEntry<AckIdV3> get(AckIdV3 key) throws SeqStoreDatabaseException {
        stateManager.lockRequiredState( BucketState.OPEN, "Cannot get from a closed or deleted Bucket" );
        try { 
            if( key.isInclusive() != null ) {
                throw new IllegalArgumentException("Cannot get an entry for an id is not an exact AckIdV3" );
            }
            
            StoredEntry<AckIdV3> stored;
            
            loadStore(false);
            try {
                stored = storeAndLock.store.get(key.getCoreAckId());
            } finally {
                releaseReadLock();
            }
            if( stored == null ) return null;
            return new StoredEntryV3(key, stored.getPayload(), stored.getLogId() );
        } finally {
            stateManager.unlock();
        }
    }
    
    @Override
    public StorePosition getPosition(AckIdV3 ackId) throws SeqStoreDatabaseException {
        stateManager.lockRequiredState( BucketState.OPEN, "Cannot get from a closed or deleted Bucket" );
        try {
            loadStore(false);
            try {
                return storeAndLock.store.getPosition( ackId );
            } finally {
                releaseReadLock();
            }
        } finally {
            stateManager.unlock();
        }
    }
    
    @Override
    public BucketIteratorImpl getIter() throws SeqStoreDatabaseException {
        stateManager.lockRequiredState( BucketState.OPEN, "Cannot get a new iterator from a closed or deleted Bucket" );
        try {
            loadStore(false);
            try {
                return storeAndLock.addIterator();
            } finally {
                releaseReadLock();
            }
        } finally {
            stateManager.unlock();
        }
    }
    
    @Override
    public int getNumIterators() {
        StoreAndLock currentStoreAndLock = storeAndLock;
        if( currentStoreAndLock == null ) return 0;
        // This could use currentStoreAndLock before its been setup or after
        // its been closed but it should still return a valid result in those
        // cases
        return currentStoreAndLock.getNumIterators();
    }
    
    @Override
    public BucketIteratorImpl copyIter(BucketIterator iter) 
            throws SeqStoreDatabaseException, IllegalStateException
    {
        stateManager.lockRequiredState( BucketState.OPEN, "Cannot copy an iterator from a closed or deleted Bucket" );
        try {
            loadStore(false);
            try {
                return storeAndLock.copyIterator( ( BucketIteratorImpl ) iter );
            } finally { 
                releaseReadLock();
            }
        } finally {
            stateManager.unlock();
        }
    }
    
    @Override
    public void closeIterator(BucketIterator iter) throws SeqStoreDatabaseException {
        BucketState state = stateManager.lock();
        try {
            if( state == BucketState.OPEN ) {
                // Close before removing from iters or the store could be closed 
                //  while the iterator is still in the process of closing
                BucketIteratorImpl iteratorImpl = ( BucketIteratorImpl ) iter;
                iteratorImpl.close();
                
                if( !acquireExistingLock(false) ) {
                    // The bucket store was already closed - this shouldn't
                    // be possible if the iterator really came from this bucket
                    return;
                }
                
                try {
                    storeAndLock.removeIterator(iteratorImpl);
                } finally {
                    releaseReadLock();
                }
            }
        } finally {
            stateManager.unlock();
        }
    }
    
    /**
     * Returns a string containing the entries in the bucket, or if the bucket is not open returns 
     * the state.
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append( super.toString() );
        result.append( "{ " );
        result.append( "id=" ).append( bucketId ).append( ", " );
        result.append( "state=" ).append( stateManager.getState() ).append( ", " );
        result.append( "storeName=" ).append( storeName ).append( ", " );
        result.append( "bucketStore=");
        if( storeAndLock != null ) {
            result.append( "<open>" );
        } else {
            result.append( "<closed>" );
        }
        result.append( ", " );
        
        result.append( "firstId=" );
        if( !firstIdLoaded ) {
            result.append( "<unknown>" );
        } else {
            result.append( firstId );
        }
        result.append( ", " );
        
        result.append( "lastId=" );
        if( !lastIdLoaded ) {
            result.append( "<unknown>" );
        } else {
            result.append( lastId );
        }
        result.append( ", " );
        
        result.append( "count= " ).append( count );
        if( !countTrusted ) {
            result.append( " (approximate) ");
        }
        result.append( ", " );
        
        result.append( "size= " ).append( byteCount );
        if( !countTrusted ) {
            result.append( " (approximate) ");
        }
        
        return result.toString(); 
    }
    
    /**
     * Closes all iterators and calls {@link #releaseResources()}
     */
    @Override
    public void close() throws SeqStoreDatabaseException {
        if( !stateManager.updateState( BucketState.OPEN, BucketState.CLOSED) ) {
            log.info( "Attempt to mark closed or deleted bucket as closed. Ignoring.");
            return;
        }
        
        if( acquireExistingLock(false) ) {
            try {
                storeAndLock.closeAllIterators();
            } finally {
                releaseReadLock();
            }
        }
        
        closeBucketStore();
    }
    
    @Override
    public BucketPersistentMetaData getMetadata() {
        // There is no valid persistent metadata until a store has been created for the bucket
        if( bucketStoreSequenceId == STORE_NOT_CREATED_SEQUENCE_ID ) {
            return null;
        }
        
        return new BucketPersistentMetaData(
                bucketStoreSequenceId, bucketId, bucketStorageType, getEntryCount(), getByteCount(),
                stateManager.getState() == BucketState.DELETED );
    }
    
    /**
     * Tells all iterators the bucket has been deleted and calls {@link #releaseResources()}
     */
    @Override
    public void deleted() throws SeqStoreDatabaseException {
        if( !stateManager.updateState( BucketState.OPEN, BucketState.DELETED) ) {
            log.info( "Attempt to mark closed or deleted bucket as deleted. Ignoring.");
            return;
        }
        
        if( acquireExistingLock(false) ) {
            try {
                storeAndLock.prepareIteratorsForDeletion();
            } finally {
                releaseReadLock();
            }
        }
        
        closeBucketStore();
    }
    
    @Override
    public BucketState getBucketState() {
        return stateManager.getState();
    }
    
    @Override
    public long getByteCount() {
        return byteCount;
    }
    
    @Override
    public long getEntryCount() {
        return count;
    }
    
    @Override
    public long getBucketStoreSequenceId() {
        if( bucketStoreSequenceId == STORE_NOT_CREATED_SEQUENCE_ID ) {
            return BucketStore.UNASSIGNED_SEQUENCE_ID;
        }
        
        return bucketStoreSequenceId;
    }
    
    @Override
    public BucketStorageType getBucketStorageType() {
        return bucketStorageType;
    }
    
    @Override
    public long getAccurateEntryCount() throws SeqStoreDatabaseException {
        stateManager.lockRequiredState( BucketState.OPEN, "Cannot get an accurate entry count from a closed or deleted Bucket" );
        try {
            if( !countTrusted ) {
                // Get the write lock so we can be sure no other thread is modifying the store
                loadStore(true);
                try {
                    countUpdater.set( this, (int) storeAndLock.store.getCount() );
                    countTrusted = true;
                } finally {
                    releaseWriteLock();
                }
            }
            
            return count;
        } finally {
            stateManager.unlock();
        }
    }
    
    @Override
    public AckIdV3 getFirstId() throws SeqStoreDatabaseException {
        stateManager.lock();
        try {
            switch( stateManager.getState() ) {
            case CLOSED:
                throw new IllegalStateException( "Cannot get first id from a closed Bucket" );
            case DELETED:
                return null;
            case OPEN:
                loadFirstId();
                return firstId;
            default:
                throw new IllegalStateException("Unrecognized state " + stateManager.getState() );
            }
        } finally {
            stateManager.unlock();
        }
    }
    
    @Override
    public AckIdV3 getLastId() throws SeqStoreDatabaseException {
        stateManager.lock();
        try {
            switch( stateManager.getState() ) {
            case CLOSED:
                throw new IllegalStateException( "Cannot get last id from a closed Bucket" );
            case DELETED:
                return null;
            case OPEN:
                loadLastId();
                return lastId;
            default:
                throw new IllegalStateException("Unrecognized state " + stateManager.getState() );
            }
        } finally {
            stateManager.unlock();
        }
    }
}
