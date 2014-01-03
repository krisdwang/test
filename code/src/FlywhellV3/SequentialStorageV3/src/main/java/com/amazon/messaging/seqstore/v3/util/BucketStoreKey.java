package com.amazon.messaging.seqstore.v3.util;

import lombok.Data;

import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

import edu.umd.cs.findbugs.annotations.NonNull;

@Data
public final class BucketStoreKey {
    @NonNull
    private final StoreId storeId;
    @NonNull
    private final AckIdV3 bucketId;
    
    private transient int hashCode;
    
    @Override
    public int hashCode() {
        if( hashCode == 0 ) {
            hashCode = storeId.hashCode() + 31 * bucketId.hashCode();
        }
        return hashCode;
    }
    
    @Override
    public boolean equals(Object o) {
        if( o == this ) return true;
        if( !(o instanceof BucketStoreKey ) ) return false;
        BucketStoreKey other = ( BucketStoreKey ) o;
        if( hashCode != 0 && other.hashCode != 0 && hashCode != other.hashCode ) return false;
        return storeId.equals( other.storeId ) && bucketId.equals( other.bucketId );
    }
}