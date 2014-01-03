package com.amazon.messaging.seqstore.v3.store;

import lombok.Getter;
import lombok.Setter;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;

/**
 * An iterator that acts as if the bucket backing it has been deleted.
 * @author stevenso
 *
 */
public class DeletedBucketIterator implements BucketIterator {
    private final StorePosition position;
    
    // Doesn't actually do anything for this implementation
    @Getter @Setter
    private boolean evictFromCacheAfterReading;
    
    public DeletedBucketIterator(StorePosition position) {
        this.position = position;
    }

    @Override
    public AckIdV3 advanceTo(AckIdV3 id) throws SeqStoreDatabaseException {
        return null;
    }
    
    @Override
    public AckIdV3 advanceTo(StorePosition position)
        throws SeqStoreDatabaseException, IllegalArgumentException
    {
        return position.getKey();
    }
    
    @Override
    public StoredEntry<AckIdV3> next() throws SeqStoreDatabaseException {
        return null;
    }

    @Override
    public AckIdV3 getBucketId() {
        return position.getBucketId();
    }

    @Override
    public StorePosition getPosition() {
        return position;
    }

    @Override
    public AckIdV3 currentKey() {
        return position.getKey();
    }
}
