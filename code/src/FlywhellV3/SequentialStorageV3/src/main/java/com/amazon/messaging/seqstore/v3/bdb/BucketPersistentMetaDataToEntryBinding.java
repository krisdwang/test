package com.amazon.messaging.seqstore.v3.bdb;

import java.util.Arrays;

import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class BucketPersistentMetaDataToEntryBinding extends TupleBinding<BucketPersistentMetaData> {
    // 4 bytes for the byte length, AckIdV3.LENGTH + 1 for the AckId plus the optional inclusive flag,
    // 16 for the entry and byte counts, plus 1 for the version, 4 for the sequence id, 
    // 1 for the storage type, 4 extra for safety  
    private static final int defaultBufferSize = 4 + AckIdV3.LENGTH + 1 + 16 + 1 + 8 + 1 + 4;
    
    private int writeVersionNumber;
    
    private boolean supportLLMFeatures;
    
    public BucketPersistentMetaDataToEntryBinding(int writeVersionNumber, boolean supportLLMFeatures) {
        if( !supportLLMFeatures && writeVersionNumber == 3 ) {
            throw new IllegalArgumentException();
        }
        setTupleBufferSize( defaultBufferSize );
        this.writeVersionNumber = writeVersionNumber;
        this.supportLLMFeatures = supportLLMFeatures;
    }
    
    @Override
    public BucketPersistentMetaData entryToObject(TupleInput input) {
        return deserialize(input, supportLLMFeatures);
    }

    public static BucketPersistentMetaData deserialize(TupleInput input, boolean supportLLMFeatures) {
        AckIdV3 bucketId = BindingUtils.readAckIdV3(input);
        long entryCount = input.readLong();
        long byteCount = input.readLong();
        long bucketSequenceId;
        BucketStorageType bucketStorageType;
        BucketPersistentMetaData.BucketState state;
        
        if( input.getBufferOffset() != input.getBufferLength() ) {
            byte versionNumber = input.readByte();
            switch( versionNumber ) {
            case 2:
            case 3:
                bucketSequenceId = input.readLong();
                byte storageTypeId = input.readByte();
                bucketStorageType = BucketStorageType.getStorageTypeForId( storageTypeId );
                if( bucketStorageType == null ) {
                    throw new BindingException(
                            "Unrecognized storage type " + storageTypeId + " is not one of " + 
                            Arrays.toString( BucketStorageType.values() ) );
                }
                
                if( versionNumber == 2 ) {
                    if( input.readBoolean() ) {
                        state = BucketPersistentMetaData.BucketState.DELETED;
                    } else {
                        state = BucketPersistentMetaData.BucketState.ACTIVE;
                    }
                } else if( !supportLLMFeatures ) {
                    throw new BindingException("Found version " + versionNumber + " that is not supported when supportLLMFeatures is false");
                } else {
                    byte stateId = input.readByte();
                    state = BucketPersistentMetaData.BucketState.getStateForId(stateId);
                }
                
                if( input.getBufferOffset() != input.getBufferLength() ) {
                    // Throw if not everything was read. The version number should have been bumped if 
                    //  there was new data
                    throw new BindingException("Unrecognized data left over in BucketPersistentMetaData store" );
                }
                break;
            default:
                throw new BindingException(
                        "Unrecognized version for BucketPersistentMetaData object in store. " +
                        "Supported versions={2,3}" +
                        " got version " + versionNumber );
            }
        } else {
            bucketSequenceId = BucketStore.UNASSIGNED_SEQUENCE_ID;
            bucketStorageType = BucketStorageType.DedicatedDatabase;
            state = BucketPersistentMetaData.BucketState.ACTIVE;
        }
        
        return new BucketPersistentMetaData(bucketSequenceId, bucketId, bucketStorageType, entryCount, byteCount, state );
    }

    @Override
    public void objectToEntry(BucketPersistentMetaData data, TupleOutput output) {
        if( writeVersionNumber < 3 && 
            data.getBucketStorageType() == BucketStorageType.DedicatedDatabaseInLLMEnv ) 
        {
            // If the bucket is in the llm env then only a version that supports the v3 format
            //  will be able to work with the bucket
            if( !supportLLMFeatures ) {
                throw new UnsupportedOperationException(
                        "BucketPersistentMetaData format requires llm support but that is disabled");
            }
            serialize(data, output, 3);
        } else {
            serialize(data, output, writeVersionNumber);
        }
    }

    private static void serialize(BucketPersistentMetaData data, TupleOutput output, int version) {
        BindingUtils.writeAckIdV3( output, data.getBucketId() );
        output.writeLong( data.getEntryCount() );
        output.writeLong( data.getByteCount() );
        
        // New to V2
        output.writeByte( version );
        output.writeLong(data.getBucketSequenceId());
        output.writeByte(data.getBucketStorageType().getId());
        if( version == 2 ) {
            if( data.getBucketState() == BucketPersistentMetaData.BucketState.CREATING ) {
                throw new IllegalArgumentException("Serializing to version 2 would loose state.");
            }
            output.writeBoolean(data.isDeleted());
        } else {
            output.writeByte(data.getBucketState().getId());
        }
    }
}
