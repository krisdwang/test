package com.amazon.messaging.seqstore.v3.bdb;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.store.BucketStore;

public abstract class BDBBackedBucketStore implements BucketStore {
    protected final AckIdV3 bucketId;
    
    protected final long sequenceId;

    public BDBBackedBucketStore(AckIdV3 bucketId, long sequenceId) {
        super();
        this.bucketId = bucketId;
        this.sequenceId = sequenceId;
    }
    
    @Override
    public AckIdV3 getBucketId() {
        return bucketId;
    }
    
    @Override
    public long getSequenceId() {
        return sequenceId;
    }
    
    protected String getNameForLogging() {
        return sequenceId + "-" + bucketId;
    }
    
    public abstract void close() throws SeqStoreDatabaseException;
}
