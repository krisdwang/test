package com.amazon.messaging.seqstore.v3;

import net.jcip.annotations.NotThreadSafe;

/**
 * This is an internal class used to merge StoredCount values.
 * @author stevenso
 *
 */
@NotThreadSafe
class PartialStoredCount {
    private long entryCount = 0;
    
    private long retainedBytesCount = 0;
    
    private long bucketCount = 0;
    
    public void add( StoredCount count ) {
        entryCount += count.getEntryCount();
        retainedBytesCount += count.getRetainedBytesCount();
        bucketCount += count.getBucketCount();
    }
    
    public StoredCount getStoredCount() {
        return new StoredCount( entryCount, retainedBytesCount, bucketCount );
    }
}