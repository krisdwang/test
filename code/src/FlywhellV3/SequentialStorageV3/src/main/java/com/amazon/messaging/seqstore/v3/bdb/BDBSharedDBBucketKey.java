package com.amazon.messaging.seqstore.v3.bdb;

import lombok.Data;

import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * The key used for records in a shared database bucket. The key is the bucket store's sequence id
 * followed by the ackid.
 * 
 * @author stevenso
 *
 */
@Data
class BDBSharedDBBucketKey {
    private static class BDBSharedDBBucketKeyBinding extends TupleBinding<BDBSharedDBBucketKey> {
        @Override
        public BDBSharedDBBucketKey entryToObject(TupleInput input) {
            long bucketSequenceId = input.readLong();
            AckIdV3 ackId = BindingUtils.readRawAckIdV3(input, -1);
            return new BDBSharedDBBucketKey(bucketSequenceId, ackId);
        }

        @Override
        public void objectToEntry(BDBSharedDBBucketKey object, TupleOutput output) {
            output.writeLong(object.bucketSequenceId);
            BindingUtils.writeRawAckIdV3(output, object.ackId);
        }
        
        @Override
        public int getTupleBufferSize() {
            return 8 + AckIdV3.LENGTH;
        }
    }
    
    public static final TupleBinding<BDBSharedDBBucketKey> BINDING = new BDBSharedDBBucketKeyBinding();
    
    /**
     * A sequence id assigned to the bucket.
     */
    private final long bucketSequenceId;
    
    private final AckIdV3 ackId;
}
