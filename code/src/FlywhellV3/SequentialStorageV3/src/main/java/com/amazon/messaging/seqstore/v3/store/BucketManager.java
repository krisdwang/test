package com.amazon.messaging.seqstore.v3.store;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.measure.unit.SI;

import lombok.Data;
import lombok.Getter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.StoredCount;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.BucketStorageManager;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.Bucket.BucketState;
import com.amazon.messaging.seqstore.v3.util.SeqStoreClosedExceptionGenerator;
import com.amazon.messaging.utils.AtomicComparableReference;
import com.amazon.messaging.utils.ClosableStateManager;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Deals with the storage buckets so we don't have to.
 * 
 * @author stevenso, robburke
 */
public class BucketManager {
    private static final Log log = LogFactory.getLog(BucketManager.class);
    
    private static final Metrics NullMetrics = new NullMetricsFactory().newMetrics();
    
    @Data
    public static class GetNextBucketIteratorResult {
        private final BucketIterator iterator;
        private final boolean reachedEnd;
    }
    
    /**
     * The minimum count is that is allowed to be innaccurate. Counts below this
     * are required to be accurate.
     */
    public static final long MIN_INACCURATE_COUNT = 1000;

    @Getter
    private final StoreId storeId;
    
    private final AtomicComparableReference<AckIdV3> cleanedUpToAckId_;

    private final BucketStorageManager bucketStorageManager;
    
    // Lock to be held while performing any action the depends on buckets not being removed or invalidated
    private final Lock bucketSetReadLock;
    
    // Lock to be held while removing or invalidating buckets
    private final Lock bucketRemoveLock;
    
    // Object to be locked when performing when doing enqueues or any other action that might affect
    //  what bucket a message would be enqueued into. This must be acquired only after all
    //  other locks have been acquired
    private final Object enqueueLock = new Object();

    private final ConcurrentNavigableMap<AckIdV3, Bucket> buckets_;
    
    private final ClosableStateManager<SeqStoreClosedException> stateManager;
    
    private volatile BucketConfig bucketConfig;
    
    public <InfoType> BucketManager(
            StoreId storeId, SeqStoreImmutableConfig config, BucketStorageManager bucketStorageManager) 
        throws SeqStoreDatabaseException 
    {
        stateManager = new ClosableStateManager<SeqStoreClosedException>(
                new SeqStoreClosedExceptionGenerator("BucketManager"), false );
        
        ReadWriteLock bucketSetReadWriteLock = new ReentrantReadWriteLock();
        bucketSetReadLock = bucketSetReadWriteLock.readLock();
        bucketRemoveLock = bucketSetReadWriteLock.writeLock();
        
        this.storeId = storeId;
        this.bucketConfig = new BucketConfig(config);
        this.bucketStorageManager = bucketStorageManager;
        cleanedUpToAckId_ = new AtomicComparableReference<AckIdV3>( AckIdV3.MINIMUM );
        
        buckets_ = bucketStorageManager.getBucketsForStore(storeId);
    }
    
    public void insert(AckIdV3 key, StoredEntryV3 msg, AckIdV3 minEnqueueLevel, boolean cache, Metrics metrics) 
            throws SeqStoreException 
    {
        if( metrics == null ) metrics = NullMetrics;
        
        if( key.getBucketPosition() != null ) {
            throw new IllegalArgumentException("Key for insertion must not have a specified position.");
        }
        
        if( key.isInclusive() != null ) {
            throw new IllegalArgumentException("Key for insertion must be exact.");
        }
        
        if( key.compareTo(minEnqueueLevel) <= 0 ) 
            throw new IllegalArgumentException("Key for insertion must be above the min enqueue level");
        
        long insertStartTime = System.nanoTime();
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("insert");
            try {
                Bucket bucket;
                synchronized (enqueueLock) {
                    Map.Entry<AckIdV3, Bucket> entry = buckets_.floorEntry(key);
                    
                    bucket = ( entry == null ) ? null : entry.getValue();
                    
                    // Cache bucketConfig to protect against it changing between calls
                    BucketConfig currentBucketConfig = bucketConfig;
                    if( bucket == null || 
                        !currentBucketConfig.couldBucketContainKey( bucket, minEnqueueLevel, cleanedUpToAckId_.get(), key) ) 
                    {
                        BucketStorageType newBucketType = getNewBucketType();
                        AckIdV3 newBucketId = currentBucketConfig.getNewBucketId(bucket, key, newBucketType);
                        assert bucket == null || bucket.getLastId().compareTo( newBucketId ) < 0 :
                            "BucketIds are incorrectly ordered : newBucketId = " + newBucketId + " lastId = " + bucket.getLastId();
                        long createBucketStartTime = System.nanoTime();
                        bucket = createBucket( newBucketId, newBucketType );
                        
                        metrics.addTime(
                                "createBucketTime", System.nanoTime() - createBucketStartTime, SI.NANO( SI.SECOND )  );
                    }
                    
                    long putStartTime = System.nanoTime();
                    // Need to hold the bucketSetAddLock while doing bucket.put as the changing
                    //   bucket size and max id could change what bucket is found for the next insert
                    bucket.put(key, msg, cache);
                    
                    metrics.addTime( 
                            "messageWriteTime", System.nanoTime() - putStartTime, SI.NANO( SI.SECOND )  );
                }
                bucketStorageManager.markBucketAsDirty(bucket);
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
            
            metrics.addTime("messageInsertTime", System.nanoTime() - insertStartTime, SI.NANO( SI.SECOND ) );
        }
    }
    
    private BucketStorageType getNewBucketType() {
        Set<BucketStorageType> supportedTypes = bucketStorageManager.getSupportedBucketTypes();
        if( supportedTypes.size() == 1 ) {
            return supportedTypes.iterator().next();
        }
        
        if( supportedTypes.contains( BucketStorageType.DedicatedDatabase ) ||
            supportedTypes.contains( BucketStorageType.DedicatedDatabaseInLLMEnv ) )
        {
            long totalBytes = 0;
            long transitionToDedicatedBucketsBytes = bucketConfig.getTransitionToDedicatedBucketsBytes();
            for( Bucket bucket : buckets_.values() ) {
                if( bucket.getBucketState() == BucketState.DELETED ) continue;
                
                totalBytes += bucket.getByteCount();
                if( totalBytes > transitionToDedicatedBucketsBytes ) {
                    if( supportedTypes.contains(BucketStorageType.DedicatedDatabaseInLLMEnv) ) {
                        return BucketStorageType.DedicatedDatabaseInLLMEnv;
                    } else {
                        return BucketStorageType.DedicatedDatabase;
                    }
                }
            }
        }
        
        if( supportedTypes.contains( BucketStorageType.SharedDatabase ) ) {
            return BucketStorageType.SharedDatabase;
        } else if( supportedTypes.contains( BucketStorageType.DedicatedDatabase ) ) {
            return BucketStorageType.DedicatedDatabase;
        } else if( supportedTypes.contains( BucketStorageType.DedicatedDatabaseInLLMEnv ) ) {
            return BucketStorageType.DedicatedDatabaseInLLMEnv;
        }
        
        throw new IllegalStateException("No known bucket types are supported");
    }
    
    /** 
     * Create a bucket with the given key and storage type. This doesn't actually persist 
     * anything to the store. That happens the  first time the bucket tries to access the 
     * bucket store, which will most likely happen on the first enqueue to the bucket.
     */
    private Bucket createBucket(AckIdV3 bucketKey, BucketStorageType storageType) throws SeqStoreException {
        Bucket bucket = bucketStorageManager.createBucket(storeId, bucketKey, storageType);
        Bucket existing = buckets_.putIfAbsent(bucketKey, bucket);
        assert existing == null;
        return bucket;
    }
    
    /**
     * @param key
     * @return The entry associated with the key.
     * @throws SeqStoreException 
     */
    public StoredEntry<AckIdV3> getMsg(AckIdV3 key) throws SeqStoreException {
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("getMsg");
            try {
                Map.Entry<AckIdV3, Bucket> entry = buckets_.floorEntry(key);
                
                return (entry == null) ? null : entry.getValue().get(key);
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
        }
    }

    /**
     * Return a bucket iterator pointing pointing to the entry with the highest
     * ack id less than or equal to key. Returns null if there is no such entry. This is
     * used for advanceTo in BucketJumper
     * 
     * @param key the key to look for
     * @param oldItr if not null this iterator will be reused if possible. If not possible then 
     *   oldItr will be closed and a new iterator returned.
     * @param evictFromCacheAfterReading used to set evictFromCacheAfterReading on the new iterator if
     *  a new iterator is created
     * @throws SeqStoreClosedException 
     * @throws SeqStoreException 
     */
    @Nullable
    public BucketIterator getBucketIteratorForKey( AckIdV3 key, BucketIterator oldItr, boolean evictFromCacheAfterReading ) 
            throws SeqStoreClosedException, SeqStoreDatabaseException 
    {
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("iterate");
            try {
                Bucket bucket = getBucketAtOrBeforeKey(key);
                
                if( bucket != null && (bucket.getFirstId() == null || key.compareTo( bucket.getFirstId() ) < 0 ) ) {
                    // Either the bucket was empty or key was before any keys in the bucket
                    for ( bucket = getPreviousBucket(bucket); bucket != null; bucket = getPreviousBucket(bucket) ) {
                        if( bucket.getLastId() != null ) {
                            // start after the last key of the previous non empty bucket.
                            // Note that if any entries are inserted into bucket 
                            // between calling bucket.getLastId() and the call
                            // to bucket.getAccurateEntryCount() the position could
                            // be off but that's always the case if the iterator is
                            // above min enqueue level and if the iterator is below 
                            // min enqueue level then there can't be any inserts 
                            // into the bucket
                            key = new AckIdV3( bucket.getLastId(), true, bucket.getAccurateEntryCount() );
                            break;
                        }
                    }
                }
                
                if( bucket == null ) {
                    if( oldItr != null ) {
                        closeBucketIterator( oldItr );
                    }
                    return null;
                }
                
                BucketIterator newItr;
                
                if( oldItr != null ) {
                    AckIdV3 oldKey = oldItr.currentKey();
                    if( oldItr.getBucketId().equals( bucket.getBucketId() ) && 
                        ( oldKey == null || oldKey.compareTo( key ) <= 0 ) ) 
                    {
                        newItr = oldItr;
                    } else {
                        closeBucketIterator( oldItr );
                        newItr = bucket.getIter();
                        newItr.setEvictFromCacheAfterReading(evictFromCacheAfterReading);
                    }
                } else {
                    newItr = bucket.getIter();
                    newItr.setEvictFromCacheAfterReading(evictFromCacheAfterReading);
                }
                
                newItr.advanceTo( key );
                
                return newItr;
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
        }
    }
    
    private Bucket getBucketAtOrBeforeKey(AckIdV3 key) {
        Map.Entry<AckIdV3, Bucket> entry = buckets_.floorEntry(key);
        if( entry == null ) return null;
        return entry.getValue();
    }
    
    /**
     * Returns the bucket immediately before the given id or null
     * if there is no such bucket
     */
    private Bucket getPreviousBucket(AckIdV3 key) {
        Map.Entry<AckIdV3, Bucket> entry = buckets_.lowerEntry(key);
        if( entry == null ) return null;
        return entry.getValue();
    }
    
    /**
     * Return the bucket immediately before the given bucket or null if the 
     * bucket is the first bucket
     */
    private Bucket getPreviousBucket(Bucket bucket) {
        return getPreviousBucket( bucket.getBucketId() );
    }
    
    public void closeBucketIterator(BucketIterator itr) throws SeqStoreDatabaseException, SeqStoreClosedException {
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("iterate");
            try {
                Bucket bucket = buckets_.get( itr.getBucketId() );
                if (bucket == null ) return; // The bucket must already have been deleted
                
                bucket.closeIterator( itr );
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
        }
    }
    
    /**
     * Return a new iterator that acts as a copy of itr.
     * @throws SeqStoreClosedException 
     */
    @Nullable
    public BucketIterator copyBucketIterator( BucketIterator itr )
            throws SeqStoreDatabaseException, SeqStoreClosedException 
    {
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("iterate");
            try {
                Bucket bucket = buckets_.get( itr.getBucketId() );
                if (bucket == null ) {
                    return new DeletedBucketIterator( itr.getPosition() );
                }
                
                return bucket.copyIter( itr );
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
        }
    }
    
    /**
     * Get the iterator for next bucket if there is one. If oldBucketItr is null this 
     * function acts returns a result with an iterator for the first bucket and
     * a reachedEnd set to false. Otherwise if there is a bucket after oldBucketItr this function 
     * returns a result with the iterator for the new bucket and reachedEnd set to false. 
     * If there is no bucket after oldBucketItr this function leaves oldBucketItr unchanged and 
     * returns a result with iterator set to the old iterator and reachedEnd set to true.
     * <p>
     * This function is used for BucketJumper.next() when it reaches the end of a bucket
     * 
     * @throws SeqStoreException if the store is closed or there is an error accessing the database.
     */
    @NonNull
    public GetNextBucketIteratorResult getNextBucketIterator(BucketIterator oldBucketItr) throws SeqStoreException {
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("iterate");
            try {
                Bucket newBucket;
                
                if( oldBucketItr == null ) {
                    newBucket = getNextBucket( null );
                } else {
                    newBucket = getNextBucket( oldBucketItr.getBucketId() );
                }
                
                if( newBucket != null ) {
                    if( oldBucketItr != null ) {
                        Bucket oldBucket = buckets_.get( oldBucketItr.getBucketId() );
                        if( oldBucket != null ) oldBucket.closeIterator( oldBucketItr );
                    }
                    return new GetNextBucketIteratorResult( newBucket.getIter(), false );
                } else {
                    return new GetNextBucketIteratorResult( oldBucketItr, true );
                }
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
        }
    }
    
    /**
     * Returns the first non empty bucket after previousBucketId. If previousBucketId returns
     * the first non empty bucket. Returns null if there is no such bucket. Must be called
     * with the bucketSetReadLock held.
     * @throws SeqStoreDatabaseException 
     */
    private Bucket getNextBucket(AckIdV3 previousBucketId) throws SeqStoreDatabaseException {
        Iterator< Bucket > itr;
        
        if( previousBucketId != null ) {
            itr = buckets_.tailMap(previousBucketId, false ).values().iterator();
        } else {
            itr = buckets_.values().iterator();
        }
        
        while( itr.hasNext() ) {
            Bucket nextBucket = itr.next();
            if( nextBucket.getFirstId() != null ) {
                return nextBucket;
            }
            log.debug( "Skipping empty bucket " + nextBucket.getBucketId() );
        }
        
        return null;
    }
    
    /**
     * Delete all messages up to but *not* including the given key. The buckets containing
     * the messages will be deleted if all messages in the bucket are deleted and there
     * is no chance that new messages could be enqueued into the buckets.
     * 
     * @param ackLevel the ackLevel below which are messages can be safely deleted
     * @param lastAvailableLevel the ackid of the maximum message that can be read or null if there is no such message
     * @param minEnqueueLevel the minimum enqueue level for the bucket manager below which no enqueues
     *   will occur. ackLevel must be at or below this.
     * @return the number of messages actually removed from the store. 
     * @throws SeqStoreDatabaseException if there is an error deleting a bucket
     * @throws SeqStoreClosedException if the bucket manager has been closed.
     * @throws IllegalArgumentException if ackLevel is greater than minEnqueueLevel
     */
    public int deleteUpTo(AckIdV3 ackLevel, AckIdV3 lastAvailableLevel, AckIdV3 minEnqueueLevel) throws SeqStoreException {
        cleanedUpToAckId_.increaseTo( ackLevel );
        
        return deleteUnneededBuckets(ackLevel, null, lastAvailableLevel, minEnqueueLevel);
    }

    /**
     * Get the count of entries covered by all buckets in the specified map. The entryCount in the returned
     * count will be adjusted by entryCountAdjustment. If the result is less than {@link #MIN_INACCURATE_COUNT}.
     * the entry count is guaranteed to be accurate
     * 
     * @param sizeAdjustment 
     */
    @NonNull
    private StoredCount getCount( ConcurrentMap<AckIdV3, Bucket> buckets, 
                                  long entryCountAdjustment, long sizeAdjustment, 
                                  long bucketAdjustment ) 
        throws SeqStoreDatabaseException, IllegalStateException 
    {
        long byteCount = sizeAdjustment;
        long entryCount = entryCountAdjustment;
        long bucketCount = bucketAdjustment;
        boolean isAccurate = true;
        
        for( Bucket bucket : buckets.values() ) {
            entryCount += bucket.getEntryCount();
            byteCount += bucket.getByteCount();
            isAccurate = isAccurate && bucket.isCountTrusted();
            bucketCount++;
        }
        
        if( entryCount < MIN_INACCURATE_COUNT && !isAccurate ) {
            bucketSetReadLock.lock(); 
            try {
                entryCount = entryCountAdjustment;
                // Recalculate byte and bucket counts as well as the buckets involved may have changed
                byteCount = sizeAdjustment; 
                bucketCount = bucketAdjustment;
                for( Bucket bucket : buckets.values() ) {
                    long oldEntryCount = bucket.getEntryCount();
                    long accurateEntryCount = bucket.getAccurateEntryCount();
                    if( accurateEntryCount != oldEntryCount ) {
                        bucketStorageManager.markBucketAsDirty(bucket);
                    }
                    
                    entryCount += accurateEntryCount;
                    byteCount += bucket.getByteCount();
                    bucketCount++;
                }
            } finally {
                bucketSetReadLock.unlock();
            }
        }
        
        return new StoredCount(entryCount, byteCount, bucketCount );
    }
    
    /**
     * Return the StorePosition for the given ackId or null if it is before all messages
     * in the store.
     * 
     * @param ackId the ackId to find the position for
     * @return the position for ackid
     * @throws SeqStoreClosedException
     * @throws SeqStoreDatabaseException
     */
    public StorePosition getStorePosition( AckIdV3 ackId ) 
            throws SeqStoreClosedException, SeqStoreDatabaseException 
    {
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("getStorePosition");
            try {
                Entry<AckIdV3, Bucket> bucketEntry = buckets_.floorEntry(ackId);
                if( bucketEntry == null ) {
                    return null;
                }
                
                return bucketEntry.getValue().getPosition( ackId );
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
        }
    }
    
    /**
     * Returns the count between from and to. A null value for either from or two is interpreted
     * as referring to the start of the store.
     * <p>
     * 
     * @throws SeqStoreException if the store is closed or there is a database error getting the counts
     */
    @NonNull
    public StoredCount getCount(StorePosition from, StorePosition to) 
            throws SeqStoreClosedException, SeqStoreDatabaseException 
    {
        stateManager.lockOpen("getCount");
        try {
            if( to == null ) return StoredCount.EmptyStoredCount;
            if( from != null && from.equals( to ) ) return StoredCount.EmptyStoredCount;
            
            long entryCountAdjustment;
            ConcurrentMap<AckIdV3, Bucket> subMap;
            
            if( from == null ) {
                subMap = buckets_.headMap( to.getBucketId(), false );
                entryCountAdjustment = to.getPosition();
            } else {
                if( from.compareTo( to ) > 0 ) return StoredCount.EmptyStoredCount;
                
                Bucket fromBucket = buckets_.get( from.getBucketId() );
                boolean includeFrom = fromBucket != null && fromBucket.getEntryCount() > from.getPosition();
                if( includeFrom ) {
                    entryCountAdjustment = to.getPosition() - from.getPosition();
                } else {
                    entryCountAdjustment = to.getPosition();
                }
                subMap = buckets_.subMap( from.getBucketId(), includeFrom, to.getBucketId(), false);
            }
            
            Bucket toBucket = buckets_.get( to.getBucketId() );
            
            long sizeAdjustment = 0;
            if( toBucket != null ) {
                sizeAdjustment = toBucket.getByteCount();
            }
            
            return getCount( subMap, entryCountAdjustment, sizeAdjustment, 1 );
        } finally {
            stateManager.unlock();
        }
    }
    
    private Bucket getFloorBucket(AckIdV3 key) {
        Map.Entry<AckIdV3, Bucket> entry = buckets_.floorEntry(key);
        
        return (entry == null) ? null : entry.getValue();
    }
    
    public StoredCount getDelayedMessageCount( @Nullable StorePosition lastAvailablePosition, long currentStoreTime) 
            throws SeqStoreDatabaseException, SeqStoreClosedException 
    {
        final AckIdV3 currentTimeAckId = new AckIdV3( currentStoreTime, true );
        
        stateManager.lockOpen("getDelayedMessageCount");
        try {
            long entryCountAdjustment;
            long sizeAdjustment;
            int bucketAdjustment;
            
            bucketSetReadLock.lock();
            try {
                Bucket firstBucket = getFloorBucket( currentTimeAckId );
                
                BucketIterator itr = null;
                if( firstBucket != null ) {
                    // Make sure the entry count is accurate before getting the enqueue lock
                    firstBucket.getAccurateEntryCount();
                    itr = firstBucket.getIter();
                }
                
                try {
                    if( itr != null && lastAvailablePosition != null &&
                        itr.getBucketId().equals( lastAvailablePosition.getBucketId() ) ) 
                    {
                        // Move the iterator as close as possible outside of the enqueue lock 
                        itr.advanceTo(lastAvailablePosition);
                    }
                
                    synchronized (enqueueLock) {
                        // Special case for no buckets
                        if( buckets_.isEmpty() ) return StoredCount.EmptyStoredCount;
                        
                        // Redo this as the first bucket may have changed
                        Bucket newFirstBucket = getFloorBucket( currentTimeAckId );
                        if( newFirstBucket != firstBucket ) {
                            if( itr != null ) {
                                assert firstBucket != null;
                                firstBucket.closeIterator(itr);
                                itr = null;
                            }
                            firstBucket = newFirstBucket;
                            if( newFirstBucket != null ) {
                                itr = newFirstBucket.getIter();
                            }
                        }
                        
                        if( firstBucket != null ) {
                            if( firstBucket.getLastId() == null || 
                                firstBucket.getLastId().compareTo( currentTimeAckId ) < 0 ) 
                            {
                                // Special case to avoid calling having to iterate if there
                                // are no messages after the current time. This should be the
                                // most common case
                                entryCountAdjustment = 0;
                                sizeAdjustment = 0;
                                bucketAdjustment = 0;
                            } else {
                                assert itr != null && itr.getBucketId().equals( firstBucket.getBucketId() );
                                
                                AckIdV3 closestId = itr.advanceTo(currentTimeAckId);
                                if( closestId == null ) {
                                    // The entire bucket is after the currentTimeAckId
                                    entryCountAdjustment = firstBucket.getEntryCount();
                                } else if( lastAvailablePosition != null && 
                                    closestId.equals( lastAvailablePosition.getKey() ) ) 
                                {
                                    // There is nothing between last available and the current time.
                                    entryCountAdjustment =
                                            firstBucket.getAccurateEntryCount() - lastAvailablePosition.getPosition();
                                } else {
                                    entryCountAdjustment = 
                                            firstBucket.getAccurateEntryCount() - itr.getPosition().getPosition();
                                }
                                
                                if( entryCountAdjustment > 0 ) {
                                    sizeAdjustment = firstBucket.getByteCount();
                                    bucketAdjustment = 1;
                                } else {
                                    sizeAdjustment = 0;
                                    bucketAdjustment = 0;
                                }
                            }
                        } else {
                            // There is no bucket with messages with an id lower than the current time
                            entryCountAdjustment = 0;
                            sizeAdjustment = 0;
                            bucketAdjustment = 0;
                        }
                    }
                } finally {
                    if( itr != null ) {
                        assert firstBucket != null;
                        firstBucket.closeIterator(itr);
                    }
                }
            } finally {
                bucketSetReadLock.unlock();
            }
            
            ConcurrentNavigableMap<AckIdV3, Bucket> subMap = buckets_.tailMap( currentTimeAckId, false );
            return getCount( subMap, entryCountAdjustment, sizeAdjustment, bucketAdjustment );
        } finally {
            stateManager.unlock();
        }
    }
    
    /**
     * Returns the number of entries after from. A null value for from is interpreted as referring to the
     * start of the store. 
     * <p>
     * @throws SeqStoreException if the store is closed or there is a database error getting the counts
     */
    @NonNull
    public StoredCount getCountAfter(StorePosition from) throws SeqStoreException {
        stateManager.lockOpen("getCountAfter");
        try {
            ConcurrentMap<AckIdV3, Bucket> subMap;
            
            long entryCountAdjustment;
            
            if( from == null ) {
                subMap = buckets_;
                entryCountAdjustment = 0;
            } else {
                Bucket fromBucket = buckets_.get( from.getBucketId() );
                boolean inclusive = fromBucket != null && fromBucket.getEntryCount() > from.getPosition();
                subMap = buckets_.tailMap( from.getBucketId(), inclusive );
                entryCountAdjustment = inclusive ? -from.getPosition() : 0;
            }
            
            return getCount( subMap, entryCountAdjustment, 0, 0 );
        } finally {
            stateManager.unlock();
        }
    }
    
    public StoredCount getCountBefore(StorePosition to) 
        throws SeqStoreDatabaseException, SeqStoreClosedException 
    {
        stateManager.lockOpen("getCountBefore");
        try {
            ConcurrentMap<AckIdV3, Bucket> subMap;
            
            long entryCountAdjustment;
            long bucketAdjustment;
            
            if( to == null ) {
                subMap = buckets_;
                entryCountAdjustment = 0;
                bucketAdjustment = 0;
            } else {
                entryCountAdjustment = to.getPosition();
                subMap = buckets_.headMap(to.getBucketId(), false);
                bucketAdjustment = 1;
            }
            
            return getCount( subMap, entryCountAdjustment, 0, bucketAdjustment );
        } finally {
            stateManager.unlock();
        }
    }
    
    public int getNumBuckets() {
        return buckets_.size();
    }
    
    /**
     * The number of dedicated database buckets for this store
     */
    public long getNumDedicatedBuckets() {
        int sum = 0;
        for( Bucket bucket : buckets_.values() ) {
            if( bucket.getBucketStorageType().isDedicated() ) {
                sum++;
            }
        }
        return sum;
    }

    /**
     * Clean up and close all buckets. This is irreversible. 
     * 
     * Any exceptions thrown while closing the buckets will be swallowed by this function.
     */
    public void close() {
        if( stateManager.close() ) {
            bucketRemoveLock.lock();
            try {
                for (AckIdV3 key : buckets_.keySet()) {
                    try {
                        Bucket bucket = buckets_.remove(key); // remove locally.
                        // This could take some time but externally synchronization
                        //  should already have stopped all enqueues before close is called
                        bucket.close();
                    } catch( Exception e ) {
                        log.error("Failed closing bucket", e );
                    }
                }
            } finally {
                try {
                    buckets_.clear();
                } finally {
                    // Make sure that bucketRemoveLock.unlock(); is called even if clear() throws
                    bucketRemoveLock.unlock();
                }
            }
        }
    }

    /**
     * Get the last message that this store was cleaned until. Equivalent to the
     * AckLevel, with the caveat that not necessarily all messages before the
     * returned message are cleared, depending on the store implementation.
     * @throws SeqStoreClosedException 
     */
    public AckIdV3 getCleanedTillLevel() throws SeqStoreClosedException {
        stateManager.checkOpen("getCleanedTillLevel");
        
        return cleanedUpToAckId_.get();
    }
    
    /**
     * Get the id of the first message stored in the first bucket, or null if there are no 
     * stored messages.
     * 
     * @throws SeqStoreClosedException if the bucket manager has been closed
     * @throws SeqStoreDatabaseException if there is an error reading from the database.
     */
    @CheckForNull
    public AckIdV3 getFirstStoredMessageId() throws SeqStoreClosedException, SeqStoreDatabaseException {
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("getFirstStoredMessageId");
            try {
                for( Bucket bucket : buckets_.values() ) {
                    AckIdV3 firstId = bucket.getFirstId();
                    if( firstId != null ) return firstId;
                }
                return null;
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
        }
    }
    
    /**
     * Close buckets stores that aren't getting used. They will be reopened on demand.
     * <p>
     * Currently unused buckets are defined as those that can't take enqueues, that
     * don't have any iterators open and that aren't immediately after 
     * a bucket that has any iterators open. The first bucket store is also not
     * closed as it is the next bucket that the furthest behind iterator will
     * read from. 
     * <p> 
     * If delay queues with long delays become popular we may have to add support 
     * for closing buckets that could get more enqueues but aren't. 
     * 
     * @param minEnqueueLevel
     * @throws SeqStoreDatabaseException
     * @throws SeqStoreClosedException
     */
    public void closeUnusedBucketStores(AckIdV3 minEnqueueLevel) 
            throws SeqStoreDatabaseException, SeqStoreClosedException 
    {
        stateManager.lockOpen("closeUnusedBucketStores");
        try {
            boolean previousHadIterator = true;
            Bucket minEnqueueBucket = getPreviousBucket( minEnqueueLevel );
            if( minEnqueueBucket == null ) {
                // There are no buckets below the min enqueue level so nothing can be closed
                return; 
            }
            
            // Only close the bucket below min enqueue level if it can't get new messages
            boolean includeMinEnqueueBucket = 
                    !bucketConfig.couldBucketContainKey(
                            minEnqueueBucket, minEnqueueLevel, cleanedUpToAckId_.get(), minEnqueueLevel);
            
            for( Bucket bucket : buckets_.headMap(minEnqueueBucket.getBucketId(), includeMinEnqueueBucket).values() ) {
                boolean hasIterator = bucket.getNumIterators() != 0;
                if( !hasIterator && !previousHadIterator ) {
                    bucket.closeBucketStore();
                } else {
                    // Preopen bucket stores after ones that have an iterator
                    try {
                        bucket.openBucketStore();
                    } catch( IllegalStateException e ) {
                        // This should be rare but it can happen that the bucket was  
                        // deleted between the iterator over buckets_ returning it 
                        // and reaching openBucketStore
                        log.debug( "Bucket was deleted when preopening it's bucket store", e );
                    }
                }
                
                previousHadIterator = hasIterator;
            }
        } finally {
            stateManager.unlock();
        }
    }
    
    /**
     * Deletes buckets that don't have any of the requiredMessages up to but not 
     * including maxDeleteLevel that don't have any of the required messages.
     * 
     * @param maxDeleteLevel the limit for deleting messages
     * @param requiredMessages the messages that must still be available after cleanup, if null
     *   all messages below maxDeleteLevel are considered elegible for deletion
     * @param lastAvailableLevel the ackid of the maximum message that can be read or null if there is no such message
     * @param minEnqueueLevel the minimum enqueue level 
     * @return the number of messages actually removed from the store. 
     * @throws SeqStoreDatabaseException if there is an error deleting a bucket
     * @throws SeqStoreClosedException if the bucket manager has been closed.
     * @throws IllegalArgumentException if maxDeleteLevel is greater than minEnqueueLevel
     */
    public int deleteUnneededBuckets(AckIdV3 maxDeleteLevel, NavigableSet<AckIdV3> requiredMessages, 
                                     AckIdV3 lastAvailableLevel, AckIdV3 minEnqueueLevel) 
        throws SeqStoreDatabaseException, SeqStoreClosedException 
    {
        if( maxDeleteLevel.compareTo( minEnqueueLevel ) > 0 )
            throw new IllegalArgumentException("Attempt to delete beyond minEnqueueLevel");
        
        if( lastAvailableLevel != null && lastAvailableLevel.compareTo( minEnqueueLevel ) > 0 ) {
            // Should never happen
            log.warn( "LastAvailable " + lastAvailableLevel + " above min enqueue level " + minEnqueueLevel );
        }
        
        stateManager.lockOpen("deleteUnneededBuckets");
        try {
            int deleteCount = 0;
            int bucketsDeletedCount = 0;
            
            Bucket maxDeletableBucket = getMaxBucketBelowKey(maxDeleteLevel);
            
            if( maxDeletableBucket == null ) {
                // Nothing can be deleted
                return 0;
            }
            
            Bucket minEnqueueBucket = getPreviousBucket( minEnqueueLevel );

            boolean lowerBucketWasSkipped = false;
            Iterator< Bucket > itr = buckets_.values().iterator();
            while( itr.hasNext() ) {
                Bucket bucket = itr.next();
                if( bucket.getBucketId().compareTo( maxDeletableBucket.getBucketId() ) > 0 )
                    break;
                
                boolean doDelete;
                bucketRemoveLock.lock();
                try {
                    // Check that no other thread has closed or deleted the bucket
                    doDelete = ( bucket.getBucketState() == Bucket.BucketState.OPEN );
                    
                    // Make sure that the bucket taking enqueues isn't deleted. This has to
                    // be done at a point where it can be guaranteed that no enqueue will
                    // pick a bucket between this thread choosing to delete the bucket
                    // and the bucket actually getting deleted or there is a risk that
                    // if bucketConfig changed this bucket could be picked for the 
                    // enqueue even if couldBucketContainKey returned false. In this
                    // case that is ensured by this thread holding the bucketRemoveLock
                    // and enqueue needing bucketSetReadLock
                    if( doDelete && bucket == minEnqueueBucket && 
                        bucketConfig.couldBucketContainKey(
                                bucket, minEnqueueLevel, cleanedUpToAckId_.get(), minEnqueueLevel) ) 
                    {
                        doDelete = false;
                    }
                    
                    if( requiredMessages != null && doDelete && 
                        bucket.getFirstId() != null && // No need to check getLastId as it can only be null if getFirstId is null
                        !requiredMessages.subSet(
                            bucket.getFirstId(), true, 
                            bucket.getLastId(), true).isEmpty() ) 
                    {
                        doDelete = false;
                    }
                    
                    // Don't delete the bucket containing lastAvailable unless all buckets before it have been deleted. 
                    // Without this lastAvailable could end up going backwards
                    if( doDelete && lowerBucketWasSkipped && bucket.getLastId() != null && lastAvailableLevel != null &&
                        bucket.getLastId().compareTo( lastAvailableLevel ) >= 0 ) 
                    {
                        doDelete = false;
                    }
                    
                    if( doDelete ) {
                        assert bucket.getLastId().compareTo( maxDeleteLevel ) < 0;
                        if( log.isDebugEnabled() ) {
                            log.debug( "Deleting bucket " + bucket.getBucketId() + " in " + storeId );
                        }
                        deleteCount += bucket.getEntryCount();
                        bucketsDeletedCount++;
                        // This just marks the bucket as deleted but doesn't delete the records yet
                        //  making it cheap enough to call within the lock
                        bucketStorageManager.deleteBucket(bucket);
                        itr.remove();
                    } else {
                        lowerBucketWasSkipped = true;
                    }
                } finally {
                    bucketRemoveLock.unlock();
                }
            }
            
            if( log.isDebugEnabled() ) {
                log.debug( "Deleted " + deleteCount + " messages from " + bucketsDeletedCount  + " buckets in " + storeId );
            }
            
            return deleteCount;
        } finally {
            stateManager.unlock();
        }
    }

    /**
     * Get the bucket with the highest id that starts before before they key and that 
     * has no entries with an id greater than or equal to the key. Note that this might 
     * return a bucket that is in the process of being deleted that had an entry greater 
     * than or equal to the key before the bucket was deleted.
     */
    private Bucket getMaxBucketBelowKey(AckIdV3 key) throws SeqStoreDatabaseException {
        Bucket maxBucket = getPreviousBucket(key);
        if( maxBucket != null ) {
            AckIdV3 lastEntryInBucket = maxBucket.getLastId();
            if( lastEntryInBucket != null && lastEntryInBucket.compareTo( key ) >= 0 ) {
                maxBucket = getPreviousBucket(maxBucket);
            }
        }
        return maxBucket;
    }

    /**
     * Return if the bucket manager is empty.
     * @return true if there are no messages available after cleanedUpToAckId_, false otherwise
     * @throws SeqStoreClosedExceptions
     * @throws SeqStoreDatabaseException
     */
    public boolean isEmpty() throws SeqStoreClosedException, SeqStoreDatabaseException {
        bucketSetReadLock.lock();
        try {
            stateManager.lockOpen("checkForEmpty"); 
            try {
                Iterator<Bucket> itr = buckets_.descendingMap().values().iterator();
                while( itr.hasNext() ) {
                    AckIdV3 lastAckId = itr.next().getLastId();
                    if( lastAckId != null ) { 
                        return lastAckId.compareTo( cleanedUpToAckId_.get() ) <= 0;
                    }
                }
                return true;
            } finally {
                stateManager.unlock();
            }
        } finally {
            bucketSetReadLock.unlock();
        }
    }
    
    private String getToStringPrefix() {
        return "BucketManager(" + storeId + ")";
    }
    
    // print out content of the bucketManager for debugging
    @Override
    public String toString() {
        try {
            if( stateManager.tryLock( 100 ) == null ) {
                return getToStringPrefix() + "{<Couldn't Get Lock>}";
            }
        } catch (InterruptedException e) {
            return getToStringPrefix() + "{<Interrupted Getting Lock>}";
        }
        
        try {
            if( !stateManager.isOpen() ) {
                return getToStringPrefix() + "{<Closed>}";
            }
            
            // Use itr and hasNext instead of empty as empty could change between the check
            //  and doing the iteration
            Iterator<Bucket> itr = buckets_.values().iterator();
            if( !itr.hasNext() ) {
                return getToStringPrefix() + "{<Empty>}";
            }
                
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            
            pw.println( getToStringPrefix() + "{" );
            while (itr.hasNext() ) {
                Bucket bucket = itr.next();
                pw.println( "\t" + 
                        bucket.getBucketId() + ":" + 
                            bucket.getFirstId() + "-" + bucket.getLastId() );
            }
            pw.println( "}" );
    
            return sw.toString();
        } catch (SeqStoreDatabaseException e) {
            return getToStringPrefix() + "{<Failed Reading Buckets " + e.getMessage() + ">}";
        } finally {
            stateManager.unlock();
        }
    }
    
    @TestOnly
    Bucket getBucket(AckIdV3 bucketId ) {
        return buckets_.get(bucketId);
    }
}
