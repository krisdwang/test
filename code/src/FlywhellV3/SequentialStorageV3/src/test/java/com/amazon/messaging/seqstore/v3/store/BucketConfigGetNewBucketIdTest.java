package com.amazon.messaging.seqstore.v3.store;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.Bucket.BucketState;
import com.amazon.messaging.testing.JMockJUnitRule;
import com.amazon.messaging.testing.LabelledParameterized;
import com.amazon.messaging.testing.LabelledParameterized.Parameters;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;
import com.google.common.collect.Lists;

@RunWith(LabelledParameterized.class)
public class BucketConfigGetNewBucketIdTest {
    @Parameters
    public static Collection<TestParameters> getBucketTypeParameter() {
        List<TestParameters> parameters = Lists.newArrayList();
        for( BucketStorageType startType : BucketStorageType.values() ) {
            for( BucketStorageType nextType : BucketStorageType.values() ) {
                parameters.add(
                        new TestParameters( 
                                startType.name() + "->" + nextType.name(), 
                                new Object[] { startType, nextType } ) );
            }
        }
        return parameters;
    }
    
    private static final BucketStoreConfig DEFAULT_DEDICATED = 
            new BucketStoreConfig.Builder().withMinPeriod(120)
                         .withMaxPeriod(300)
                         .withMaxEnqueueWindow(300)
                         .withMinSize(10*1024) // 10 mb
                         .build();
    
    private static final BucketStoreConfig DEFAULT_SHARED = 
            new BucketStoreConfig.Builder().withMinPeriod(60)
                         .withMaxPeriod(180)
                         .withMaxEnqueueWindow(60)
                         .withMinSize(5*1024) // 5 mb
                         .build();
    
    private static final int transitionPointBytes = 20 * 1024;
    
    private static final BucketConfig config;
    
    static {
        SeqStoreConfig seqStoreConfig = new SeqStoreConfig();
        seqStoreConfig.setSharedDBBucketStoreConfig(DEFAULT_SHARED);
        seqStoreConfig.setDedicatedDBBucketStoreConfig(DEFAULT_DEDICATED);
        seqStoreConfig.setMaxSizeInKBBeforeUsingDedicatedDBBuckets(transitionPointBytes);
        config = new BucketConfig(seqStoreConfig);
    }
    
    private final Mockery context = new JUnit4Mockery();
    
    @Rule
    public final JMockJUnitRule rule = new JMockJUnitRule(context);
    
    private final AckIdGenerator generator = new AckIdGenerator();
    
    private final BucketStorageType oldBucketType;
    
    private final BucketStorageType nextBucketType;
    
    private final BucketStoreConfig oldBucketTypeConfig;
    
    private final BucketStoreConfig nextBucketTypeConfig;
    
    private static BucketStoreConfig getBucketConfig( BucketStorageType type ) {
        switch( type ) {
        case SharedDatabase:
            return DEFAULT_SHARED;
        case DedicatedDatabase:
        case DedicatedDatabaseInLLMEnv:
            return DEFAULT_DEDICATED;
        default:
            throw new IllegalArgumentException();
        }
    }
    
    public BucketConfigGetNewBucketIdTest(BucketStorageType oldBucketType, BucketStorageType nextBucketType) {
        this.oldBucketType = oldBucketType;
        this.nextBucketType = nextBucketType;
        oldBucketTypeConfig = getBucketConfig(oldBucketType);
        nextBucketTypeConfig = getBucketConfig(nextBucketType);
    }
    
    private AckIdV3 ackId( long time ) {
        return generator.getAckId(time);
    }
    
    private long minOldPeriodMS() {
        return oldBucketTypeConfig.getMinPeriod() * 1000l;
    }
    
    private long minNewPeriodMS() {
        return nextBucketTypeConfig.getMinPeriod() * 1000l;
    }
    
    private long maxOldEnqueueWindowMS() {
        return oldBucketTypeConfig.getMaxEnqueueWindow() * 1000l;
    }
    
    private FakeBucketBuilder bucketBuilder() {
        return new FakeBucketBuilder()
            .withBucketType(oldBucketType);
    }
    
    private void runTest( Bucket previousBucket, AckIdV3 ackId ) 
            throws SeqStoreDatabaseException, IllegalStateException 
    {
        AckIdV3 bucketId = config.getNewBucketId(previousBucket, ackId, nextBucketType);
        
        AckIdV3 defaultStartId = 
                new AckIdV3( ( ackId.getTime() / minNewPeriodMS() ) * minNewPeriodMS(), false );
        
        AckIdV3 afterPrevLastId = null;
        AckIdV3 afterPrevMinEndId = null;
        if( previousBucket != null ) {
            if( previousBucket.getLastId() !=null ) {
                afterPrevLastId = new AckIdV3( previousBucket.getLastId(), true );
            }
            
            long prevMinEndTime = previousBucket.getBucketId().getTime() + minOldPeriodMS();
            afterPrevMinEndId = new AckIdV3( prevMinEndTime, false );
        }
        
        AckIdV3 maxId = AckIdV3.max( AckIdV3.max(defaultStartId, afterPrevLastId), afterPrevMinEndId );
        assertEquals( maxId, bucketId );
    }
    
    @Test
    public void testNoOldBucket() throws SeqStoreDatabaseException, IllegalStateException {
        runTest( null, ackId( minNewPeriodMS() * 5 + 40 ) );
    }
    
    @Test
    public void testFarAwayFromOldBucket() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(ackId(1010))
                .build(context);
        
        // way out of range for the config
        long newBucketTime = 1000 + 5 * ( minOldPeriodMS() + maxOldEnqueueWindowMS() ); 
        runTest( bucket, ackId( newBucketTime ) );
    }
    
    @Test
    public void testFarAwayFromOldEmptyBucket() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(null)
                .build(context);
        
        // way out of range for the config
        long newBucketTime = 1000 + 5 * ( minOldPeriodMS() + maxOldEnqueueWindowMS() ); 
        runTest( bucket, ackId( newBucketTime ) );
    }
    
    @Test
    public void testImmediatelyAfterEmptyNonExtendedBucket() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(null)
                .build(context);
        
        // way out of range for the config
        long newBucketTime = 1000 + minOldPeriodMS(); 
        runTest( bucket, ackId( newBucketTime ) );
    }
    
    @Test
    public void testImmediatelyAfterNonEmptyNonExtendedBucket() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(ackId(1010))
                .build(context);
        
        long newBucketTime = 1000 + minOldPeriodMS(); 
        runTest( bucket, ackId( newBucketTime ) );
    }
    
    @Test
    public void testImmediatelyAfterExtendedBucket() throws SeqStoreDatabaseException, IllegalStateException {
        long lastIdTime = 1005 + minOldPeriodMS(); 
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(ackId(lastIdTime))
                .build(context);
        
        runTest( bucket, ackId( lastIdTime ) );
    }
    
    @Test
    public void testFarAfterExtendedBucket() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(ackId(1005 + minOldPeriodMS()))
                .build(context);
        
        long newBucketTime = 1005 + 2 * minNewPeriodMS(); 
        runTest( bucket, ackId( newBucketTime ) );
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testOverlappingMinPeriod() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(ackId(1006))
                .build(context);
        
        config.getNewBucketId(bucket, ackId( 1000 + minOldPeriodMS() - 1 ), nextBucketType);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testOverlappingLastId() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(ackId(1000 + minOldPeriodMS() + 10 ) )
                .build(context);
        
        config.getNewBucketId(bucket, ackId( 1000 + minOldPeriodMS() + 9 ), nextBucketType);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testAfterClosed() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
                .withStartTime(1000)
                .withLastId(ackId(1000 + minOldPeriodMS() + 10 ) )
                .withBucketState(BucketState.CLOSED)
                .build(context);
        
        config.getNewBucketId(bucket, ackId( 1000 + minOldPeriodMS() + 30 ), nextBucketType);
    }
}
