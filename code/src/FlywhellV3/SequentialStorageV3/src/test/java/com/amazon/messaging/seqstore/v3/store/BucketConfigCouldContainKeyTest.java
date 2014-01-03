package com.amazon.messaging.seqstore.v3.store;

import static org.junit.Assert.*;

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
public class BucketConfigCouldContainKeyTest {
    @Parameters
    public static Collection<TestParameters> getBucketTypeParameter() {
        List<TestParameters> parameters = Lists.newArrayList();
        for( BucketStorageType bucketType : BucketStorageType.values() ) {
            parameters.add(
                    new TestParameters( 
                            bucketType.name(), 
                            new Object[] { bucketType } ) );
        }
        return parameters;
    }
    
    private static final BucketStoreConfig DEFAULT_DEDICATED = 
            new BucketStoreConfig.Builder().withMinPeriod(120)
                         .withMaxPeriod(1000)
                         .withMaxEnqueueWindow(300)
                         .withMinSize(10*1024) // 10 mb
                         .build();
    
    private static final BucketStoreConfig DEFAULT_SHARED = 
            new BucketStoreConfig.Builder().withMinPeriod(60)
                         .withMaxPeriod(2000)
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
    
    private final BucketStorageType bucketType;
    
    private final BucketStoreConfig bucketTypeConfig;
    
    public BucketConfigCouldContainKeyTest(BucketStorageType bucketType) {
        this.bucketType = bucketType;
        switch( bucketType ) {
        case SharedDatabase:
            bucketTypeConfig = DEFAULT_SHARED;
            break;
        case DedicatedDatabase:
        case DedicatedDatabaseInLLMEnv:
            bucketTypeConfig = DEFAULT_DEDICATED;
            break;
        default:
            throw new IllegalArgumentException();
        }
    }
    
    private AckIdV3 ackId( long time ) {
        return generator.getAckId(time);
    }
    
    private long minPeriodMS() {
        return bucketTypeConfig.getMinPeriod() * 1000l;
    }
    
    private long maxEnqueueWindowMS() {
        return bucketTypeConfig.getMaxEnqueueWindow() * 1000l;
    }
    
    private long maxPeriodMS() {
        return bucketTypeConfig.getMaxPeriod() * 1000l;
    }
    
    private long minSizeBytes() {
        return bucketTypeConfig.getMinimumSize() * 1024l;
    }
    
    private FakeBucketBuilder bucketBuilder() {
        return new FakeBucketBuilder()
            .withBucketType(bucketType);
    }
    
    @Test
    public void testTransitionPoint() throws SeqStoreDatabaseException {
        assertEquals( transitionPointBytes * 1024l, config.getTransitionToDedicatedBucketsBytes() );
    }

    @Test
    public void testCouldContainInMinRange() throws SeqStoreDatabaseException, IllegalStateException {
        // Only the start time matters if its in range.
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1001), ackId( 1500 ), ackId( 1000 + minPeriodMS() - 1 ) );
        assertEquals( true, couldContainKey ); 
    }
    
    @Test
    public void testCouldContainAlreadyCovered() throws SeqStoreDatabaseException, IllegalStateException {
        long lastIdTime = 1000 + 5 * ( minPeriodMS() + maxEnqueueWindowMS() ); // way out of range for the config 
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId( lastIdTime ) ) 
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1001), ackId( 1500 ), ackId( lastIdTime - 1 ) );
        assertEquals( true, couldContainKey ); 
    }
    
    @Test
    public void testCanExtendEmpty() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(null)
               .withLastId(null)
               .withSize(0)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1000 + minPeriodMS() - 1 ), ackId( 900 ), ackId( 1000 + minPeriodMS() + 1 ) );
        assertEquals( true, couldContainKey ); 
    }
    
    @Test
    public void testCanExtendAlmostFull() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId(1010))
               .withSize( minSizeBytes() - 1 )
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1000 + minPeriodMS() - 1 ), ackId( 900 ), ackId( 1000 + minPeriodMS() ) );
        assertEquals( true, couldContainKey ); 
    }
    
    @Test
    public void testCantExtendFull() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId(1010))
               .withSize(minSizeBytes())
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1000 + minPeriodMS() - 1 ), ackId( 900 ), ackId( 1000 + minPeriodMS() ) );
        assertEquals( false, couldContainKey ); 
    }
    
    @Test
    public void testCanExtendJustBeforeMaxEnqueueWindow() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId(1010))
               .withSize(10)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1000 + minPeriodMS() - 1 ), 
                        ackId( 900 ), ackId( 1000 + minPeriodMS() + maxEnqueueWindowMS() - 1 ) );
        assertEquals( false, couldContainKey ); 
    }
    
    @Test
    public void testCantExtendBeyondMaxEnqueueWindow() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId(1010))
               .withSize(10)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1000 + minPeriodMS() - 1 ), 
                        ackId( 900 ), ackId( 1000 + minPeriodMS() + maxEnqueueWindowMS() ) );
        assertEquals( false, couldContainKey ); 
    }
    
    @Test
    public void testCantExtendBeyondMaxTime() throws SeqStoreDatabaseException, IllegalStateException {
        long maxTime = 1000 + maxPeriodMS();
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId(1010))
               .withSize(10)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( maxTime - 1 ), 
                        ackId( 900 ), ackId( maxTime ) );
        assertEquals( false, couldContainKey ); 
    }
    
    // Test that extending a bucket that could still take enqueues is allowed even if all currently
    //  enqueued messages are deletable
    @Test
    public void testCanExtendAlmostDeletable() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId(1010))
               .withSize(10)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1000 + minPeriodMS() - 2 ), 
                        ackId( 1000 + minPeriodMS() - 1 ), ackId( 1000 + minPeriodMS() + 1 ) );
        assertEquals( true, couldContainKey ); 
    }
    
    @Test
    public void testCantExtendDeletable() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId(1010))
               .withSize(10)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1000 + minPeriodMS() + 2 ), 
                        ackId( 1000 + minPeriodMS() - 1 ), ackId( 1000 + minPeriodMS() + 3 ) );
        assertEquals( false, couldContainKey ); 
    }
    
    @Test
    public void testCantExtendDeletabled() throws SeqStoreDatabaseException, IllegalStateException {
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .withFirstId(ackId(1001))
               .withLastId(ackId(1010))
               .withSize(10)
               .withBucketState(BucketState.DELETED)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 1000 + minPeriodMS() - 2 ), 
                        ackId( 900 ), ackId( 1000 + minPeriodMS() + 1 ) );
        assertEquals( false, couldContainKey ); 
    }
    
    @Test
    public void testCantContainBeforeStart() throws SeqStoreDatabaseException, IllegalStateException {
        // Only the start time matters if its in range.
        Bucket bucket = bucketBuilder()
               .withStartTime(1000)
               .build(context);
        boolean couldContainKey = 
                config.couldBucketContainKey(
                        bucket, ackId( 900), ackId( 800 ), ackId( 950 ) );
        assertEquals( false, couldContainKey ); 
    }
}
