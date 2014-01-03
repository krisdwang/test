package com.amazon.messaging.seqstore.v3.store;

import net.jcip.annotations.Immutable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfigInterface;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.Bucket.BucketState;

@Immutable @EqualsAndHashCode @ToString
public class BucketConfig {
    private final BucketStoreConfig dedicatedBucketConfig;
    
    private final BucketStoreConfig sharedBucketConfig;
    
    @Getter
    private final long transitionToDedicatedBucketsBytes;
    
    public BucketConfig(SeqStoreConfigInterface seqStoreConfig) {
        dedicatedBucketConfig = seqStoreConfig.getDedicatedDBBucketStoreConfig();
        sharedBucketConfig = seqStoreConfig.getSharedDBBucketStoreConfig();
        transitionToDedicatedBucketsBytes = seqStoreConfig.getMaxSizeInKBBeforeUsingDedicatedDBBuckets() * 1024;
    }
    
    private BucketStoreConfig getBucketConfigForType(BucketStorageType bucketType) {
        switch( bucketType ) {
        case SharedDatabase:
            return sharedBucketConfig;
        case DedicatedDatabase:
        case DedicatedDatabaseInLLMEnv:
            return dedicatedBucketConfig;
        default:
            throw new IllegalArgumentException("Unrecognized bucket storage type " + bucketType ); 
        }
    }
    
    /**
     * Returns if bucket could contain the specified key. The result of this
     * function can change based on actions to the bucket. Callers should also 
     * be aware that the BucketConfig instance for a BucketManager could
     * change to new config which could obviously result in a different result.
     * 
     * @param bucket the bucket to check if it could contain the key
     * @param minEnqueueLevel the minimum enqueue level for the store
     * @param cleanLevel the clean level for the store. This must be below minEnqueueLevel
     * @param key the key to check
     * @throws SeqStoreDatabaseException if there is an error retrieving data from bucket
     * @throws IllegalStateException if the bucket is closed.
     */
    public boolean couldBucketContainKey(Bucket bucket, AckIdV3 minEnqueueLevel, AckIdV3 cleanLevel, AckIdV3 key) 
            throws SeqStoreDatabaseException, IllegalStateException 
    {
        long keyTime = key.getTime();
        long bucketTime = bucket.getBucketId().getTime();
        
        if( keyTime < bucketTime ) {
            return false; // Before the start of the bucket
        }
        
        BucketStoreConfig bucketConfig = getBucketConfigForType(bucket.getBucketStorageType());
        
        long minBucketEnd = bucketTime + bucketConfig.getMinPeriod() * 1000l;  
        
        // The key is within the minimum size of the bucket
        if( keyTime < minBucketEnd )
            return true;
        
        // This will throw IllegalStateException if the bucket is closed
        AckIdV3 lastId = bucket.getLastId();
        
        if( lastId != null && key.compareTo(lastId) <= 0 ) {
            return true; // The bucket already covers the key
        }

        long minEnqueueTime = minEnqueueLevel.getTime();
        if( minEnqueueTime >= minBucketEnd && ( lastId == null || cleanLevel.compareTo(lastId) > 0 ) ) {
            // Don't extend buckets that are already eligible for deletion.
            return false;
        }
        
        long totalTime = keyTime - bucketTime;
        if( totalTime >= ( bucketConfig.getMaxPeriod() * 1000l ) ) {
            return false;
        }
            
        if (bucket.getByteCount() >= ( bucketConfig.getMinimumSize() * 1024l ) ) {
            return false; // Previous bucket is full so don't extend it
        }
        
        long bucketMinEnqueueTime = Math.max( minEnqueueTime, bucketTime );
        
        long newEnqueueWindow = keyTime - bucketMinEnqueueTime;
        if( newEnqueueWindow >= ( bucketConfig.getMaxEnqueueWindow() * 1000l ) ) {
            return false;
        }
        
        // If everything is working right I don't think the bucket can be anything other than
        // open but check for that just to be safe
        return bucket.getBucketState() == BucketState.OPEN; // Extend the bucket if it hasn't been deleted
    }
    
    /**
     * Get the bucket id for a new  bucket containing key given that previousBucket is the bucket
     * immediately before key and that previousBucket should not contain key. Returns the lowest
     * possible id that is greater than all of the lower bound bucket id for the key, the minimum end point
     * of the previous bucket and the id of the last message in the previous bucket. Note that this
     * does allow the returned bucket id to be in the range that the previous bucket could be extended
     * to cover effectively limiting its expansion.
     * 
     * @throws SeqStoreDatabaseException if there is an error retrieving data from previousBucket
     * @throws IllegalArgumentException if previousBucket has been closed or deleted or 
     *    if key is before the end of previousBucket
     */
    public AckIdV3 getNewBucketId(Bucket previousBucket, AckIdV3 key, BucketStorageType bucketType) 
            throws SeqStoreDatabaseException, IllegalStateException
    {
        BucketStoreConfig currentBucketConfig = getBucketConfigForType(bucketType);
        
        long bucketPeriod = currentBucketConfig.getMinPeriod() * 1000l;
        long maxMultipleOfPeriodLessThanKeyTime = ( key.getTime() / bucketPeriod ) * bucketPeriod; 
        
        if( previousBucket != null ) {
            BucketStoreConfig prevBucketConfig = getBucketConfigForType(previousBucket.getBucketStorageType());
            long lastBucketMinEndTime = 
                    previousBucket.getBucketId().getTime() + 1000l * prevBucketConfig.getMinPeriod();
            
            AckIdV3 lastId = previousBucket.getLastId();
            if( (lastId != null && lastId.compareTo( key ) >= 0 ) || lastBucketMinEndTime > key.getTime() ) {
                throw new IllegalArgumentException("key must be after the end of previousBucket");
            }
            
            // We can do just one check here as at this point we've got all that is needed from 
            // previousBucket so it doesn't matter if another thread closes or deletes it
            // after this point. The caller should prevent that from happening anyway
            // so this is just a safety check.
            if( previousBucket.getBucketState() != BucketState.OPEN ) {
                throw new IllegalArgumentException( "previousBucket was closed or deleted in getNewBucketId" );
            }
            
            if( lastId != null && lastId.getTime() >= lastBucketMinEndTime &&
                    maxMultipleOfPeriodLessThanKeyTime <= lastId.getTime() ) 
            {
                // Start the new bucket immediately after the previous bucket
                return new AckIdV3( lastId, true );
            } else if( maxMultipleOfPeriodLessThanKeyTime <= lastBucketMinEndTime ) {
                return new AckIdV3( lastBucketMinEndTime, false );
            }
        }
        
        return new AckIdV3( maxMultipleOfPeriodLessThanKeyTime, false );
    }
}
