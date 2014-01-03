package com.amazon.messaging.seqstore.v3;

import java.util.Collection;

import javax.annotation.concurrent.Immutable;

import com.amazon.messaging.seqstore.v3.store.BucketManager;

import lombok.Data;
import lombok.ToString;

/**
 * A data class used to return information about the number of messages
 * and bytes between two locations in a store.
 * 
 * @author stevenso
 *
 */
@Data
@Immutable
@ToString(includeFieldNames=false)
public class StoredCount {
    public static final StoredCount EmptyStoredCount = new StoredCount(0, 0, 0);
    
    private final long entryCount;
    
    private final long retainedBytesCount;
    
    private final long bucketCount;

    public StoredCount(long entryCount, long retainedBytesCount, long bucketCount) {
        this.entryCount = entryCount;
        this.retainedBytesCount = retainedBytesCount;
        this.bucketCount = bucketCount;
    }
    
    /**
     * Create a {@link StoredCount} that is the sum of multiple StoredCount objects
     */
    public StoredCount( Collection< StoredCount > counts ) {
        long tmpEntryCount = 0;
        long tmpRetainedBytesCount = 0;
        long tmpBucketCount = 0;
        for( StoredCount count : counts ) {
            tmpEntryCount += count.getEntryCount();
            tmpRetainedBytesCount += count.getRetainedBytesCount();
            tmpBucketCount += count.getBucketCount();
        }
        entryCount = tmpEntryCount;
        retainedBytesCount = tmpRetainedBytesCount;
        bucketCount = tmpBucketCount;
    }
    
    /**
     * The number of entries between the two points. May be slightly low but will never
     * be inaccurate below {@link BucketManager#MIN_INACCURATE_COUNT}.
     * 
     * @return
     */
    public long getEntryCount() {
        return entryCount;
    }
    
    /**
     * Get the number of bytes that have to be retained to keep the
     * entries available. This will be the sum of the number of bytes in all buckets 
     * that are used by any message in the in entryCount. This number may be slightly 
     * under the true size after a restart until buckets active at the time
     * of the shutdown are outside of the range measured.
     */
    public long getRetainedBytesCount() {
        return retainedBytesCount;
    }
}
