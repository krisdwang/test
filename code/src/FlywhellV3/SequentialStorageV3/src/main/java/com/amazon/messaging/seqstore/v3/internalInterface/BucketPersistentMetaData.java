package com.amazon.messaging.seqstore.v3.internalInterface;


import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

import edu.umd.cs.findbugs.annotations.NonNull;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * The metadata describing a bucket that should be persisted to the datastore and available after
 * a restart. The metrics may be inaccurate but should always be lower than the true value
 * 
 * @author stevenso
 *
 */
@Data @RequiredArgsConstructor @Immutable
public class BucketPersistentMetaData {
    public enum BucketState {
        ACTIVE(0),
        DELETED(1),
        CREATING(2);
        
        private static Map<Byte, BucketState> idToBucketStateMap;
        
        static {
            idToBucketStateMap = new HashMap<Byte, BucketState>();
            for( BucketState val : values() ) {
                idToBucketStateMap.put( val.getId(), val);
            }
        }
        
        public static BucketState getStateForId(byte id) {
            return idToBucketStateMap.get(id);
        }
        
        private BucketState(int id) {
            this.id = (byte)id;
        }
        
        private byte id;
        
        
        public byte getId() {
            return id;
        }
    }
    
    private final long bucketSequenceId;
    @NonNull
    private final AckIdV3 bucketId;
    @NonNull
    private final BucketStorageType bucketStorageType;
    private final long entryCount;
    private final long byteCount;
    
    /**
     * Flag to indicate the bucket state. This is used as part of cross environment synchronization
     * and to allow recording that the bucket is scheduled to be deleted in the background.
     */
    private final BucketState bucketState;
    
    /**
     * Create a BucketPersistentMetaData for an empty bucket
     * @param ackId
     */
    public BucketPersistentMetaData(long bucketSequenceId, AckIdV3 bucketId, BucketStorageType bucketStorageType, BucketState initialState) {
        if( bucketId == null ) throw new NullPointerException();
        if( bucketStorageType == null ) throw new NullPointerException();
        
        this.bucketSequenceId = bucketSequenceId;
        this.bucketId = bucketId;
        this.bucketStorageType = bucketStorageType;
        this.entryCount = 0;
        this.byteCount = 0;
        this.bucketState = initialState;
    }
    
    public BucketPersistentMetaData(long bucketSequenceId, AckIdV3 bucketId,
                                     BucketStorageType bucketStorageType, long entryCount, long byteCount,
                                     boolean deleted)
    {
        super();
        this.bucketSequenceId = bucketSequenceId;
        this.bucketId = bucketId;
        this.bucketStorageType = bucketStorageType;
        this.entryCount = entryCount;
        this.byteCount = byteCount;
        this.bucketState = deleted ? BucketState.DELETED : BucketState.ACTIVE;
    }
    
    /**
     * Get a copy of the this BucketPersistentMetaData with the deleted flag set to true.
     * @return
     */
    public BucketPersistentMetaData getDeletedMetadata() {
        return copyWithNewState(BucketState.DELETED);
    }
    
    public boolean isDeleted() {
        return bucketState == BucketState.DELETED;
    }
    
    public boolean isActive() {
        return bucketState == BucketState.ACTIVE;
    }
    
    public boolean isCreating() {
        return bucketState == BucketState.CREATING;
    }
    
    /**
     * Get a copy of the this BucketPersistentMetaData with the new state
     * @return
     */
    public BucketPersistentMetaData copyWithNewState(BucketState newState) {
        if( bucketState == newState ) return this;
        
        return new BucketPersistentMetaData(
                bucketSequenceId, bucketId, bucketStorageType, entryCount, byteCount, newState);
    }
}
