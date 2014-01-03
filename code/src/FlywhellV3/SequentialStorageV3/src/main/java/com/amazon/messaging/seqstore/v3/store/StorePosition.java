package com.amazon.messaging.seqstore.v3.store;

import lombok.Data;
import lombok.NonNull;

import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

/**
 * A class that represents the location of an iterator within a store.
 * <p>
 * StorePosition are comparable with other StorePositions for the same store. Comparisons
 * with positions from different stores will not fail but will not produce meaningful 
 * results. Also note that just because one position is before another does not mean
 * that there are any messages between them.
 * 
 * @author stevenso
 *
 */
@Data
public class StorePosition implements Comparable<StorePosition> {
    @NonNull
    private final AckIdV3 bucketId;
    
    /**
     * The key of the entry at this position, null if the position is before any entries in the bucket. The key
     * is always an exact entry and will always include the bucket position.
     */
    private final AckIdV3 key;
    
    public StorePosition(AckIdV3 bucketId, AckIdV3 key) {
        if( bucketId == null ) throw new IllegalArgumentException( "bucketId cannot be null" );
        if( key != null ) {
            if( key.getBucketPosition() == null ) throw new IllegalArgumentException( "key must have a position" );
            if( key.isInclusive() != null ) throw new IllegalArgumentException( "key must be exact" );
        }
        this.bucketId = bucketId;
        this.key = key;
    }
    
    /**
     * The position in the bucket. 0 means before any entries in the bucket.
     */
    public long getPosition() {
        if( key == null ) return 0;
        return key.getBucketPosition();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + bucketId.hashCode();
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        
        StorePosition other = (StorePosition) obj;
        if (!bucketId.equals(other.bucketId))
            return false;
        
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        } else {
            assert( key.getBucketPosition().equals( other.key.getBucketPosition() ) );
        }
    
        return true;
    }
    
    @Override
    public int compareTo(StorePosition o) {
        int bucketIdCompare = bucketId.compareTo( o.bucketId );
        if( bucketIdCompare != 0 ) return bucketIdCompare;
        
        if( key == null ) {
            if( o.key == null ) return 0;
            return -1;
        } else if( o.key == null ) {
            return 1;
        } else {
            return key.compareTo( o.key );
        }
    }
}