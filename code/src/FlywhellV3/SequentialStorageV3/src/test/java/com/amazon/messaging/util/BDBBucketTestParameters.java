package com.amazon.messaging.util;

import java.util.Arrays;
import java.util.List;

import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;


public class BDBBucketTestParameters {
    
    
    public static List<TestParameters> getAllOptionsTestParameters() {
        return Arrays.asList( 
                new TestParameters( "Dedicated,AllowLLM", new Object[] { BucketStorageType.DedicatedDatabase, true } ),
                new TestParameters( "Dedicated,!AllowLLM", new Object[] { BucketStorageType.DedicatedDatabase, false } ),
                new TestParameters( "LLDedicated,AllowLLM", new Object[] { BucketStorageType.DedicatedDatabaseInLLMEnv, true } ),
                new TestParameters( "LLDedicated,!AllowLLM", new Object[] { BucketStorageType.DedicatedDatabaseInLLMEnv, false } ),
                new TestParameters( "Shared,AllowLLM", new Object[] { BucketStorageType.SharedDatabase, true }),
                new TestParameters( "Shared,!AllowLLM", new Object[] { BucketStorageType.SharedDatabase, false }));
    }
    
    public static List<TestParameters> getStorageTypeTestParameters() {
        return Arrays.asList(  
                new TestParameters( "DedicatedLLM", new Object[] { BucketStorageType.DedicatedDatabaseInLLMEnv } ),
                new TestParameters( "Dedicated", new Object[] { BucketStorageType.DedicatedDatabase } ),
                new TestParameters( "Shared", new Object[] { BucketStorageType.SharedDatabase } ) );
    }
}
