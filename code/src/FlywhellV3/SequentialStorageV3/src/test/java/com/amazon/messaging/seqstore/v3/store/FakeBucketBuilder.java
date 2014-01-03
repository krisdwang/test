package com.amazon.messaging.seqstore.v3.store;

import org.jmock.Expectations;
import org.jmock.Mockery;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.Bucket.BucketState;

class FakeBucketBuilder {
    private AckIdV3 bucketId;
    private boolean bucketIdSet = false;
    
    private AckIdV3 firstId;
    private boolean firstIdSet = false;
    
    private AckIdV3 lastId;
    private boolean lastIdSet = false;
    
    private long size;
    private boolean sizeSet = false;
    
    private BucketStorageType bucketType;
    private boolean bucketTypeSet = false;
    
    private BucketState bucketState = BucketState.OPEN;
    
    public FakeBucketBuilder withBucketId( AckIdV3 bucketId ) {
        this.bucketId = bucketId;
        bucketIdSet = true;
        return this;
    }
    
    public FakeBucketBuilder withStartTime(long time) {
        this.bucketId = new AckIdV3(time, false);
        bucketIdSet = true;
        return this;
    }
    
    public FakeBucketBuilder withFirstId( AckIdV3 firstId ) {
        this.firstId = firstId;
        firstIdSet = true;
        return this;
    }

    public FakeBucketBuilder withLastId( AckIdV3 lastId ) {
        this.lastId = lastId;
        lastIdSet = true;
        return this;
    }
    
    public FakeBucketBuilder withRange( AckIdV3 firstId, AckIdV3 lastId ) {
        return withFirstId(firstId).withLastId(lastId);
    }

    public FakeBucketBuilder withSize( long size ) {
        this.size = size;
        sizeSet = true;
        return this;
    }

    public FakeBucketBuilder withBucketState( BucketState bucketState ) {
        this.bucketState = bucketState;
        return this;
    }

    public FakeBucketBuilder withBucketType( BucketStorageType bucketType ) {
        this.bucketType = bucketType;
        bucketTypeSet = true;
        return this;
    }
    
    public Bucket build(Mockery context) {
        final Bucket bucket = context.mock(Bucket.class, bucketId.toString() );
        try {
            context.checking( new Expectations(){{
                if( bucketIdSet ) {
                    allowing( bucket ).getBucketId(); will( returnValue( bucketId ) );
                }
                if( firstIdSet ) {
                    allowing( bucket ).getFirstId(); will( returnValue( firstId ) );
                }
                if( lastIdSet ) {
                    allowing( bucket ).getLastId(); will( returnValue( lastId ) );
                }
                if( bucketTypeSet ) {
                    allowing( bucket ).getBucketStorageType(); will( returnValue( bucketType  ) );
                }
                if( sizeSet ) {
                    allowing( bucket ).getByteCount(); will( returnValue( size ) );
                }
                allowing( bucket ).getBucketState(); will( returnValue( bucketState ) );
            }} );
        } catch (SeqStoreDatabaseException e) {
            throw new RuntimeException(e);
        }
        return bucket;
    }
}