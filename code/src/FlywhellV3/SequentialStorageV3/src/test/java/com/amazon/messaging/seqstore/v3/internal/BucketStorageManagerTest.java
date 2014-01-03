package com.amazon.messaging.seqstore.v3.internal;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.Bucket;
import com.amazon.messaging.seqstore.v3.store.Bucket.BucketState;
import com.amazon.messaging.testing.DeterministicScheduledExecutorService;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.util.StubBucket;
import com.amazon.messaging.util.StubStorePersistenceManager;
import com.amazon.messaging.utils.Scheduler;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class BucketStorageManagerTest extends TestCase {
    private static final SeqStoreImmutablePersistenceConfig testConfig;
    
    static {
        SeqStorePersistenceConfig tmpConfig = new SeqStorePersistenceConfig();
        tmpConfig.setStoreDirectory(new File("/tmp/"));
        tmpConfig.setNumDedicatedDBDeletionThreads(1);
        tmpConfig.setNumSharedDBDeletionThreads(1);
        tmpConfig.setOpenReadOnly(false);
        tmpConfig.setDirtyMetadataFlushPeriod(10000);
        testConfig = tmpConfig.getImmutableConfig();
    }
    
    private static class TestThreadFactory implements ThreadFactory {
        private AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
        private List<Thread> createdThreads = Collections.synchronizedList( Lists.<Thread>newArrayList() );
        
        @Override
        public Thread newThread(final Runnable r) {
            Thread retval = new Thread( new Runnable() {
                @Override
                public void run() {
                    try {
                        r.run();
                    } catch( Throwable e ) {
                        firstException.compareAndSet(null, e);
                    }
                }
            });
            retval.setDaemon(true);
            createdThreads.add(retval);
            return retval;
        }
        
        public List<Thread> getCreatedThreads() {
            synchronized (createdThreads) {
                return Lists.newArrayList( createdThreads );
            }
        }
        
        public void rethrow() {
            Throwable e = firstException.get();
            if( e == null ) return;
            
            if( e instanceof AssertionError ) {
                AssertionError error = new AssertionError( e.getMessage() );
                error.initCause(e);
                throw error;
            } else {
                throw new RuntimeException( e );
            }
        }
    }
    
    @Data
    private static final class DeletedBucketData {
        private final StoreId storeId;
        private final AckIdV3 bucketId;
    }
    
    @Data
    private static final class UpdatedBucketData {
        private final StoreId storeId;
        private final BucketPersistentMetaData data;
    }
    
    private static class TestStubStorePersistenceManager extends StubStorePersistenceManager {
        protected final List<DeletedBucketData> deletedBuckets = Collections.synchronizedList( Lists.<DeletedBucketData>newArrayList() );
        
        protected final List<UpdatedBucketData> updatedMetadata = 
                Collections.synchronizedList( Lists.<UpdatedBucketData>newArrayList() );
        
        
        public TestStubStorePersistenceManager() {
            super(testConfig);
        }
        
        @Override
        public int getNumBuckets() throws SeqStoreDatabaseException {
            return 0;
        }
        
        @Override
        public int getNumDedicatedBuckets() throws SeqStoreDatabaseException {
            return 0;
        }
        
        @Override
        public boolean deleteBucketStore(StoreId storeId, AckIdV3 bucketId)
            throws SeqStoreDatabaseException, IllegalStateException
        {
            deletedBuckets.add( new DeletedBucketData( storeId, bucketId ) );
            return true;
        }
        
        @Override
        public boolean updateBucketMetadata(StoreId destinationId, BucketPersistentMetaData metadata)
            throws SeqStoreDatabaseException
        {
            updatedMetadata.add( new UpdatedBucketData( destinationId, metadata ) );
            return true;
        }
        

        public List<DeletedBucketData> getDeletedBuckets() {
            synchronized (deletedBuckets) {
                return Lists.newArrayList( deletedBuckets );
            }
        }
        
        
        public List<UpdatedBucketData> getUpdatedMetadata() {
            synchronized ( updatedMetadata ) {
                return Lists.newArrayList( updatedMetadata );
            }
        }
    }
    
    private static final int bucketSeperatorTime = 60000;
    
    private final AtomicInteger bucketSequenceSource = new AtomicInteger(1);
    
    private final Random random = new Random(1);
    
    private List<BucketPersistentMetaData> getFakeBucketMetadata( long startTime, int numBuckets, boolean deleted ) 
    {
        return getFakeBucketMetadata(startTime, numBuckets, BucketStorageType.SharedDatabase, deleted );
    }
    
    private List<BucketPersistentMetaData> getFakeBucketMetadata( long startTime, int numBuckets, BucketStorageType type, boolean deleted ) 
    {
        List<BucketPersistentMetaData> fakeBucketMetadata = Lists.newArrayList();
        long time = startTime;
        for( int i = 0; i < numBuckets; ++i ) {
            fakeBucketMetadata.add( getFakeBucketMetadata(time, type, deleted) );
            time += bucketSeperatorTime;
        }
        
        return fakeBucketMetadata;
    }

    private BucketPersistentMetaData getFakeBucketMetadata(long time, boolean deleted) {
        return getFakeBucketMetadata(time, BucketStorageType.SharedDatabase, deleted);
    }
    
    private BucketPersistentMetaData getFakeBucketMetadata(long time, BucketStorageType bucketStorageType, boolean deleted) {
        return new BucketPersistentMetaData(
                bucketSequenceSource.getAndIncrement(), 
                new AckIdV3(time, false), 
                bucketStorageType,
                random.nextInt(1000), random.nextInt(10000), // Size and count 
                deleted );
    }
    
    private static void checkBuckets( 
        List<BucketPersistentMetaData> sourceMetadata, 
        ConcurrentNavigableMap<AckIdV3, Bucket> returnedBuckets ) 
    {
        int expectedBuckets = 0;
        for( BucketPersistentMetaData metadata : sourceMetadata ) {
            if( metadata.isDeleted() ) continue;
            
            expectedBuckets++;
            Bucket matchingBucket = returnedBuckets.get( metadata.getBucketId() );
            assertNotNull( matchingBucket );
            
            assertEquals( metadata.getBucketId(), matchingBucket.getBucketId() );
            assertEquals( metadata.getEntryCount(), matchingBucket.getEntryCount() );
            assertEquals( metadata.getByteCount(), matchingBucket.getByteCount() );
            assertEquals( metadata.getBucketStorageType(), matchingBucket.getBucketStorageType() );
        }
        
        if( expectedBuckets != returnedBuckets.size() ) {
            fail( "Got unexpected buckets in " + returnedBuckets + " for metadata " + sourceMetadata );
        }
    }

    @Test
    public void testGetBucketsForStoreWithNoDeletions() throws SeqStoreDatabaseException, InterruptedException {
        final StoreId storeId = new StoreIdImpl("testStore");
        
        final List<BucketPersistentMetaData> fakeBucketMetadata = getFakeBucketMetadata( 1000, 5, false );
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public java.util.Collection<BucketPersistentMetaData> getBucketMetadataForStore(StoreId destinationId) 
                    throws SeqStoreDatabaseException 
            {
                assertEquals( storeId, destinationId );
                return fakeBucketMetadata;
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        ConcurrentNavigableMap<AckIdV3, Bucket> buckets = bucketStoreManager.getBucketsForStore(storeId);
        checkBuckets( fakeBucketMetadata, buckets );
        
        assertEquals( 0, bucketStoreManager.getSharedDBBucketDeleteQueueSize() );
        assertEquals( 0, bucketStoreManager.getDedicatedDBBucketDeleteQueueSize() );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        
        assertTrue( persistenceManager.getDeletedBuckets().isEmpty() );
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );
    }
    
    @Test
    public void testGetBucketsForStoreWithDeletedBuckets() throws Exception {
        final StoreId storeId = new StoreIdImpl("testStore");
        
        int numDeletedBuckets = 2 + testConfig.getNumSharedDBDeletionThreads();
        final List<BucketPersistentMetaData> deletedBucketMetadata = getFakeBucketMetadata( 1000, numDeletedBuckets, true );
        final List<BucketPersistentMetaData> activeBucketMetadata = getFakeBucketMetadata( 1000+6+bucketSeperatorTime, 3, false);
        
        final List<BucketPersistentMetaData> allBucketMetadata = Lists.newArrayList();
        allBucketMetadata.addAll( deletedBucketMetadata );
        allBucketMetadata.addAll( activeBucketMetadata );
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public java.util.Collection<BucketPersistentMetaData> getBucketMetadataForStore(StoreId destinationId) 
                    throws SeqStoreDatabaseException 
            {
                assertEquals( storeId, destinationId );
                return allBucketMetadata;
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        ConcurrentNavigableMap<AckIdV3, Bucket> buckets = bucketStoreManager.getBucketsForStore(storeId);
        checkBuckets( allBucketMetadata, buckets );
        
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        
        HashSet<DeletedBucketData> expectedDeletions = new HashSet<DeletedBucketData>();
        for( BucketPersistentMetaData metadata : deletedBucketMetadata ) {
            expectedDeletions.add( new DeletedBucketData( storeId, metadata.getBucketId() ) );
        }
        assertEquals( expectedDeletions, new HashSet<DeletedBucketData>( persistenceManager.getDeletedBuckets() ) );
        
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );
    }
    
    @Test
    public void testDeleteBucketStore() throws Exception {
        final StoreId storeId = new StoreIdImpl("testStore");
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager();
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        BucketPersistentMetaData metadata = getFakeBucketMetadata(1000, false);
        
        Bucket fakeBucket = new StubBucket(storeId, metadata);
        bucketStoreManager.deleteBucket( fakeBucket );
        
        assertEquals( BucketState.DELETED, fakeBucket.getBucketState() );
        assertEquals( Collections.singletonList( new UpdatedBucketData(storeId, metadata.getDeletedMetadata() ) ), 
                      persistenceManager.getUpdatedMetadata() );
        
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        assertEquals( Collections.singletonList( new DeletedBucketData( storeId, metadata.getBucketId() ) ), 
                      persistenceManager.getDeletedBuckets() );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
    }
    
    @Test
    public void testDeleteBucketStoreOrder() throws Exception {
        final StoreId storeId1 = new StoreIdImpl("testStore1");
        final StoreId storeId2 = new StoreIdImpl("testStore2");
        
        final CountDownLatch scheduledDeletions = new CountDownLatch(1);
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean deleteBucketStore(StoreId storeId, AckIdV3 bucketId) throws SeqStoreDatabaseException ,IllegalStateException {
                try {
                    if( !scheduledDeletions.await(1, TimeUnit.SECONDS) ) {
                        throw new SeqStoreDatabaseException("Timedout waiting for scheduledDeletions" );
                    }
                } catch (InterruptedException e) {
                    throw new SeqStoreDatabaseException("Interrupted waiting for scheduledDeletions" );
                }
                return super.deleteBucketStore(storeId, bucketId);
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        int baseTime1 = 200;
        Set<AckIdV3> deletedStore1Buckets = Sets.newHashSet();
        
        int numStore1Buckets = 5;
        for( int i = 0; i < numStore1Buckets; i++ ) {
            BucketPersistentMetaData metadata = getFakeBucketMetadata(baseTime1 + bucketSeperatorTime * i, false);
            Bucket bucket = new StubBucket(storeId1, metadata);
            bucketStoreManager.deleteBucket(bucket);
            
            List<UpdatedBucketData> updates = persistenceManager.getUpdatedMetadata();
            assertEquals( new UpdatedBucketData(storeId1, metadata.getDeletedMetadata() ), updates.get( updates.size() - 1 ) );
            
            assertEquals( BucketState.DELETED, bucket.getBucketState() );
            deletedStore1Buckets.add( bucket.getBucketId() );
            
            
        }
        
        Set<AckIdV3> deletedStore2Buckets = Sets.newHashSet();
        int baseTime2 = 100;
        
        int numStore2Buckets = 7;
        for( int i = 0; i < numStore2Buckets; i++ ) {
            BucketPersistentMetaData metadata = getFakeBucketMetadata(baseTime2 + bucketSeperatorTime * i, false);
            Bucket bucket = new StubBucket(storeId2, metadata);
            bucketStoreManager.deleteBucket(bucket);
            
            List<UpdatedBucketData> updates = persistenceManager.getUpdatedMetadata();
            assertEquals( new UpdatedBucketData(storeId2, metadata.getDeletedMetadata() ), updates.get( updates.size() - 1 ) );
            
            assertEquals( BucketState.DELETED, bucket.getBucketState() );
            deletedStore2Buckets.add( bucket.getBucketId() );
        }
        
        scheduledDeletions.countDown();
        
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        assertEquals( numStore1Buckets + numStore2Buckets, persistenceManager.getDeletedBuckets().size() );
        
        // Check that deletes are in order from newest to oldest except for the first one which could
        //  have started before the full set was added
        long lastTime = -1;
        int bucketCount = 0;
        for( DeletedBucketData data : persistenceManager.getDeletedBuckets() ) {
            if( bucketCount > 1 ) { // The first bucket may be out of order
                assertTrue( data.getBucketId().getTime() < lastTime );
            }
            bucketCount++;
            lastTime = data.getBucketId().getTime();
            
            if( data.getStoreId().equals( storeId1 ) ) {
                assertTrue( deletedStore1Buckets.contains( data.getBucketId() ) );
            } else if( data.getStoreId().equals( storeId2 ) ) {
                assertTrue( deletedStore2Buckets.contains( data.getBucketId() ) );
            } else {
                fail( "Unexpected store " + data.getStoreId() );
            }
        }
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
    }
    
    @Test
    public void testDeleteBucketsForStore() throws Exception {
        final StoreId storeId = new StoreIdImpl("testStore1");
        
        final CountDownLatch scheduledDeletions = new CountDownLatch(1);
        
        final List<BucketPersistentMetaData> fakeBucketMetadata = getFakeBucketMetadata( 1000, 5, true );
        
        final TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(
                StoreId paramStoreId) throws SeqStoreDatabaseException, IllegalStateException
            {
                assertEquals( storeId, paramStoreId );
                return fakeBucketMetadata;
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        final boolean[] completionTaskRun = new boolean[] { false };
        
        bucketStoreManager.deleteBucketsForStore(storeId, 
                new Runnable() {
                    @Override
                    public void run() {
                        HashSet<DeletedBucketData> expectedDeletions = new HashSet<DeletedBucketData>();
                        for( BucketPersistentMetaData metadata : fakeBucketMetadata ) {
                            expectedDeletions.add( new DeletedBucketData( storeId, metadata.getBucketId() ) );
                        }
                        assertEquals( expectedDeletions, new HashSet<DeletedBucketData>( persistenceManager.getDeletedBuckets() ) );
                        
                        completionTaskRun[0] = true;
                    }
                });
        
        
        scheduledDeletions.countDown();
        
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        assertTrue( completionTaskRun[0] );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
    }
    
    @Test
    public void testDeleteBucketStoreThrowsStoreException() throws Exception {
        final StoreId storeId = new StoreIdImpl("testStore");
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean deleteBucketStore(StoreId storeIdParam, AckIdV3 bucketId) throws SeqStoreDatabaseException ,IllegalStateException {
                super.deleteBucketStore(storeIdParam, bucketId);
                throw new SeqStoreDatabaseException("Testing failure");
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        BucketPersistentMetaData metadata = getFakeBucketMetadata(1000, false);
        
        Bucket fakeBucket = new StubBucket(storeId, metadata);
        bucketStoreManager.deleteBucket( fakeBucket );
        
        assertEquals( BucketState.DELETED, fakeBucket.getBucketState() );
        assertEquals( Collections.singletonList( new UpdatedBucketData(storeId, metadata.getDeletedMetadata() ) ), 
                      persistenceManager.getUpdatedMetadata() );
        
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        DeletedBucketData deleteBucketData = new DeletedBucketData( storeId, metadata.getBucketId() );
        // The delete should be tried MAX_DELETE_ATTEMPTS times before giving up 
        assertEquals( Collections.nCopies( BucketStorageManager.MAX_DELETE_ATTEMPTS, deleteBucketData ),
                      persistenceManager.getDeletedBuckets() );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
    }
    
    @Test
    public void testDeleteBucketStoreThrowsRuntimeException() throws Exception {
        final StoreId storeId = new StoreIdImpl("testStore");
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean deleteBucketStore(StoreId storeIdParam, AckIdV3 bucketId) throws SeqStoreDatabaseException ,IllegalStateException {
                super.deleteBucketStore(storeIdParam, bucketId);
                throw new RuntimeException("Testing failure");
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        BucketPersistentMetaData metadata = getFakeBucketMetadata(1000, false);
        
        Bucket fakeBucket = new StubBucket(storeId, metadata);
        bucketStoreManager.deleteBucket( fakeBucket );
        
        assertEquals( BucketState.DELETED, fakeBucket.getBucketState() );
        assertEquals( Collections.singletonList( new UpdatedBucketData(storeId, metadata.getDeletedMetadata() ) ), 
                      persistenceManager.getUpdatedMetadata() );
        
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        DeletedBucketData deleteBucketData = new DeletedBucketData( storeId, metadata.getBucketId() );
        // The delete should be tried MAX_DELETE_ATTEMPTS times before giving up 
        assertEquals( Collections.nCopies( BucketStorageManager.MAX_DELETE_ATTEMPTS, deleteBucketData ),
                      persistenceManager.getDeletedBuckets() );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
    }
    
    @Test
    public void testDeleteBucketStoreThrowsUnrecoverableException() throws Exception {
        final StoreId storeId = new StoreIdImpl("testStore");
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean deleteBucketStore(StoreId storeIdParam, AckIdV3 bucketId) throws SeqStoreDatabaseException ,IllegalStateException {
                super.deleteBucketStore(storeIdParam, bucketId);
                throw new SeqStoreUnrecoverableDatabaseException("Testing failure", "Failure");
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        BucketPersistentMetaData metadata1 = getFakeBucketMetadata(1000, BucketStorageType.SharedDatabase, false);
        Bucket fakeBucket1 = new StubBucket(storeId, metadata1);
        bucketStoreManager.deleteBucket( fakeBucket1 );
        
        BucketPersistentMetaData metadata2 = getFakeBucketMetadata(1200, BucketStorageType.DedicatedDatabase, false);
        Bucket fakeBucket2 = new StubBucket(storeId, metadata2);
        bucketStoreManager.deleteBucket( fakeBucket2 );
        
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        DeletedBucketData deleteBucketData1 = new DeletedBucketData( storeId, metadata1.getBucketId() );
        DeletedBucketData deleteBucketData2 = new DeletedBucketData( storeId, metadata2.getBucketId() );
        // One try for each type then give up and shutdown the thread 
        assertEquals( Sets.newHashSet( deleteBucketData1, deleteBucketData2 ),
                      Sets.newHashSet( persistenceManager.getDeletedBuckets() ) );
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        bucketStoreManager.shutdown();
        
        testThreadFactory.rethrow();
    }
    
    @Test
    public void testShutdown() throws SeqStoreDatabaseException, InterruptedException {
        final StoreId storeId = new StoreIdImpl("testStore1");
        
        final CountDownLatch allowDeletes = new CountDownLatch(1);
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean deleteBucketStore(StoreId storeIdParam, AckIdV3 bucketId) throws SeqStoreDatabaseException ,IllegalStateException {
                try {
                    if( !allowDeletes.await(1, TimeUnit.SECONDS) ) {
                        throw new SeqStoreDatabaseException("Timedout waiting for scheduledDeletions" );
                    }
                } catch (InterruptedException e) {
                    throw new SeqStoreDatabaseException("Interrupted waiting for scheduledDeletions" );
                }
                return super.deleteBucketStore(storeIdParam, bucketId);
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        final BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        Set<AckIdV3> deletedRequestedBuckets = Sets.newHashSet();
        
        int numBuckets = 5;
        for( int i = 0; i < numBuckets; i++ ) {
            BucketPersistentMetaData metadata = getFakeBucketMetadata(200 + bucketSeperatorTime * i, false);
            Bucket bucket = new StubBucket(storeId, metadata);
            bucketStoreManager.deleteBucket(bucket);
            
            List<UpdatedBucketData> updates = persistenceManager.getUpdatedMetadata();
            assertEquals( new UpdatedBucketData(storeId, metadata.getDeletedMetadata() ), updates.get( updates.size() - 1 ) );
            
            assertEquals( BucketState.DELETED, bucket.getBucketState() );
            deletedRequestedBuckets.add( bucket.getBucketId() );
        }
        
        TestExecutorService testExecutorService = new TestExecutorService();
        testExecutorService.submit( new Runnable() {
            @Override
            public void run() {
                bucketStoreManager.shutdown();
            }
        });
        
        // Give a little time for bucketStoreManager.shutdown() to start 
        Thread.sleep(50);
        allowDeletes.countDown();

        testExecutorService.shutdownWithin(1, TimeUnit.SECONDS);
        testExecutorService.rethrow();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        
        if( !persistenceManager.getDeletedBuckets().isEmpty() ) {
            // Only the delete that started before shutdown was called should have happened
            assertEquals( 1, persistenceManager.getDeletedBuckets().size() );
            assertTrue( deletedRequestedBuckets.contains( persistenceManager.getDeletedBuckets().get(0).getBucketId() ) );
        }
        
        // The queues should have been cleared and only contain the shutdown key
        assertEquals( 1, bucketStoreManager.getDedicatedDBBucketDeleteQueueSize() );
        assertEquals( 1, bucketStoreManager.getSharedDBBucketDeleteQueueSize() );
    }
    
    @Test
    public void testShutdownWithStuckThread() throws SeqStoreDatabaseException, InterruptedException {
        final StoreId storeId = new StoreIdImpl("testStore1");
        
        final CountDownLatch allowDeletes = new CountDownLatch(1);
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean deleteBucketStore(StoreId storeIdParam, AckIdV3 bucketId) throws SeqStoreDatabaseException ,IllegalStateException {
                try {
                    if( !allowDeletes.await(10, TimeUnit.SECONDS) ) {
                        throw new SeqStoreDatabaseException("Timedout waiting for scheduledDeletions" );
                    }
                } catch (InterruptedException e) {
                    throw new SeqStoreDatabaseException("Interrupted waiting for scheduledDeletions" );
                }
                return super.deleteBucketStore(storeIdParam, bucketId);
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        final BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        Set<AckIdV3> deletedRequestedBuckets = Sets.newHashSet();
        
        int numBuckets = 5;
        for( int i = 0; i < numBuckets; i++ ) {
            BucketPersistentMetaData metadata = getFakeBucketMetadata(200 + bucketSeperatorTime * i, false);
            Bucket bucket = new StubBucket(storeId, metadata);
            bucketStoreManager.deleteBucket(bucket);
            
            List<UpdatedBucketData> updates = persistenceManager.getUpdatedMetadata();
            assertEquals( new UpdatedBucketData(storeId, metadata.getDeletedMetadata() ), updates.get( updates.size() - 1 ) );
            
            assertEquals( BucketState.DELETED, bucket.getBucketState() );
            deletedRequestedBuckets.add( bucket.getBucketId() );
        }
        
        TestExecutorService testExecutorService = new TestExecutorService();
        testExecutorService.submit( new Runnable() {
            @Override
            public void run() {
                bucketStoreManager.shutdown();
            }
        });
        
        long startTime = System.nanoTime();
        
        testExecutorService.shutdownWithin(6, TimeUnit.SECONDS);
        testExecutorService.rethrow();
        
        long shutdownTimeMillis = TimeUnit.NANOSECONDS.toMillis( System.nanoTime() - startTime );
        assertTrue( "Shutdown in " + shutdownTimeMillis, shutdownTimeMillis > 4500 && shutdownTimeMillis < 6500 );
        
        allowDeletes.countDown();
        
        // The threads should all shutdown quickly once unblocked
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            testThread.join( 100 );
            assertFalse( testThread.isAlive() );
        }
        
        testThreadFactory.rethrow();
        
        if( !persistenceManager.getDeletedBuckets().isEmpty() ) {
            // Only the delete that started before shutdown was called should have happened
            assertEquals( 1, persistenceManager.getDeletedBuckets().size() );
            assertTrue( deletedRequestedBuckets.contains( persistenceManager.getDeletedBuckets().get(0).getBucketId() ) );
        }
        
        // The queues should have been cleared and only contain the shutdown key
        assertEquals( 1, bucketStoreManager.getDedicatedDBBucketDeleteQueueSize() );
        assertEquals( 1, bucketStoreManager.getSharedDBBucketDeleteQueueSize() );
    }
    
    @Test
    public void testUpdateBucketMetadata() throws SeqStoreDatabaseException {
        final DeterministicScheduledExecutorService executorService = new DeterministicScheduledExecutorService();
        final Scheduler scheduler = new Scheduler("FakeSchedulerThread", executorService );
        final TestThreadFactory testThreadFactory = new TestThreadFactory();
        final StoreId storeId1 = new StoreIdImpl("store1");
        final StoreId storeId2 = new StoreIdImpl("store2");
        final long flushPeriod = testConfig.getDirtyMetadataFlushPeriod();
        
        final TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, scheduler, testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        // The background flush should be scheduled
        assertEquals( 1, scheduler.numRemainingTasks() );
        
        final Bucket bucket1 = new StubBucket(storeId1, getFakeBucketMetadata(100, false) );
        final Bucket bucket2 = new StubBucket(storeId2, getFakeBucketMetadata(200, false) );
        
        bucketStoreManager.markBucketAsDirty(bucket1);
        bucketStoreManager.markBucketAsDirty(bucket2);
        
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );

        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        UpdatedBucketData expectedUpdate1 = new UpdatedBucketData(storeId1, bucket1.getMetadata());
        UpdatedBucketData expectedUpdate2 = new UpdatedBucketData(storeId2, bucket2.getMetadata());
        
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        // Second run should do nothing
        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        assertEquals( 0, scheduler.numRemainingTasks() );
    }
    
    @Test
    public void testDeleteCancelsUpdate() throws SeqStoreDatabaseException {
        final DeterministicScheduledExecutorService executorService = new DeterministicScheduledExecutorService();
        final Scheduler scheduler = new Scheduler("FakeSchedulerThread", executorService );
        final TestThreadFactory testThreadFactory = new TestThreadFactory();
        final StoreId storeId1 = new StoreIdImpl("store1");
        final StoreId storeId2 = new StoreIdImpl("store2");
        final long flushPeriod = testConfig.getDirtyMetadataFlushPeriod();
        
        final TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, scheduler, testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        BucketPersistentMetaData bucket1Metadata = getFakeBucketMetadata(100, false);
        BucketPersistentMetaData bucket2Metadata = getFakeBucketMetadata(200, false);
        
        final Bucket bucket1 = new StubBucket(storeId1, bucket1Metadata);
        final Bucket bucket2 = new StubBucket(storeId2, bucket2Metadata);
        
        bucketStoreManager.markBucketAsDirty(bucket1);
        bucketStoreManager.markBucketAsDirty(bucket2);
        bucketStoreManager.deleteBucket(bucket1);
        
        // The delete update should show up immediately
        UpdatedBucketData expectedUpdate1 = new UpdatedBucketData(storeId1, bucket1Metadata.getDeletedMetadata());
        assertEquals( Sets.newHashSet( expectedUpdate1 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        executorService.tick(flushPeriod + 1, TimeUnit.MILLISECONDS);
        
        // Only the bucket2 update should happen - the bucket1 update should have been skipped
        UpdatedBucketData expectedUpdate2 = new UpdatedBucketData(storeId2, bucket2Metadata);
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        assertEquals( 0, scheduler.numRemainingTasks() );
    }
    
    @Test
    public void testDedupMetadataUpdate() throws SeqStoreDatabaseException {
        final DeterministicScheduledExecutorService executorService = new DeterministicScheduledExecutorService();
        final Scheduler scheduler = new Scheduler("FakeSchedulerThread", executorService );
        final TestThreadFactory testThreadFactory = new TestThreadFactory();
        final StoreId storeId1 = new StoreIdImpl("store1");
        final StoreId storeId2 = new StoreIdImpl("store2");
        final long flushPeriod = testConfig.getDirtyMetadataFlushPeriod();
        
        final TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, scheduler, testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        BucketPersistentMetaData initialBucket1Metadata = getFakeBucketMetadata(100, false);
        final StubBucket bucket1 = new StubBucket(storeId1, initialBucket1Metadata);
        final StubBucket bucket2 = new StubBucket(storeId2, getFakeBucketMetadata(200, false));
        
        bucketStoreManager.markBucketAsDirty(bucket1);
        bucketStoreManager.markBucketAsDirty(bucket2);
        
        bucket1.setEntryCount( bucket1.getEntryCount() + 10 );
        bucket1.setByteCount( bucket1.getByteCount() + 200 );
        BucketPersistentMetaData newBucket1Metadata = bucket1.getMetadata();
        assertFalse( newBucket1Metadata.equals( initialBucket1Metadata ) );
        
        bucketStoreManager.markBucketAsDirty(bucket1);
        
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );

        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        UpdatedBucketData expectedUpdate1 = new UpdatedBucketData(storeId1, newBucket1Metadata);
        UpdatedBucketData expectedUpdate2 = new UpdatedBucketData(storeId2, bucket2.getMetadata());
        
        // Only 2 buckets even if update was called twice for bucket1 
        assertEquals( 2, persistenceManager.getUpdatedMetadata().size() );
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        // Second run should do nothing
        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        assertEquals( 2, persistenceManager.getUpdatedMetadata().size() );
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        bucket1.setEntryCount( bucket1.getEntryCount() + 10 );
        bucket1.setByteCount( bucket1.getByteCount() + 200 );
        BucketPersistentMetaData thirdBucket1Metadata = bucket1.getMetadata();
        assertFalse( thirdBucket1Metadata.equals( newBucket1Metadata ) );
        
        // A change after the flush has happened should not be deduped
        bucketStoreManager.markBucketAsDirty(bucket1);
        
        UpdatedBucketData expectedUpdate3 = new UpdatedBucketData(storeId1, thirdBucket1Metadata);
        
        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        assertEquals( 3, persistenceManager.getUpdatedMetadata().size() );
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2, expectedUpdate3 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        assertEquals( 0, scheduler.numRemainingTasks() );
    }
    
    @Test
    public void testNoDedupeIfUpdateIsInProgress() throws SeqStoreDatabaseException {
        final DeterministicScheduledExecutorService executorService = new DeterministicScheduledExecutorService();
        final Scheduler scheduler = new Scheduler("FakeSchedulerThread", executorService );
        final TestThreadFactory testThreadFactory = new TestThreadFactory();
        final StoreId storeId = new StoreIdImpl("store1");
        final long flushPeriod = testConfig.getDirtyMetadataFlushPeriod();
        
        final Runnable afterUpdateTask[] = new Runnable[] { null };
        
        final TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean updateBucketMetadata(StoreId destinationId, BucketPersistentMetaData metadata)
                throws SeqStoreDatabaseException
            {
                boolean retval = super.updateBucketMetadata(destinationId, metadata);
                if( afterUpdateTask[0] != null ) {
                    afterUpdateTask[0].run();
                }
                return retval;
            }
        };
        
        final BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, scheduler, testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        final BucketPersistentMetaData initialBucket1Metadata = getFakeBucketMetadata(100, false);
        final StubBucket bucket1 = new StubBucket(storeId, initialBucket1Metadata);
        
        bucketStoreManager.markBucketAsDirty(bucket1);
        
        afterUpdateTask[0] = new Runnable() {
            @Override
            public void run() {
                bucket1.setEntryCount( bucket1.getEntryCount() + 100 );
                bucket1.setByteCount( bucket1.getByteCount() + 1000);
                bucketStoreManager.markBucketAsDirty(bucket1);
            }
        };
        
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );

        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        UpdatedBucketData expectedUpdate1 = new UpdatedBucketData(storeId, initialBucket1Metadata);
        
        // Only 2 buckets even if update was called twice for bucket1 
        assertEquals( 1, persistenceManager.getUpdatedMetadata().size() );
        assertEquals( Sets.newHashSet( expectedUpdate1 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        final BucketPersistentMetaData newBucket1Metadata = bucket1.getMetadata();
        assertFalse( newBucket1Metadata.equals( initialBucket1Metadata ) );
        
        // Don't schedule a third update
        afterUpdateTask[0] = null;
        
        // Second run should update again
        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        UpdatedBucketData expectedUpdate2 = new UpdatedBucketData(storeId, newBucket1Metadata);
        assertEquals( 2, persistenceManager.getUpdatedMetadata().size() );
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        // Third run should do nothing
        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        assertEquals( 2, persistenceManager.getUpdatedMetadata().size() );
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        assertEquals( 0, scheduler.numRemainingTasks() );
    }
    
    @Test
    public void testUpdateForClosedAndDeletedBuckets() throws SeqStoreDatabaseException {
        final DeterministicScheduledExecutorService executorService = new DeterministicScheduledExecutorService();
        final Scheduler scheduler = new Scheduler("FakeSchedulerThread", executorService );
        final TestThreadFactory testThreadFactory = new TestThreadFactory();
        final StoreId storeId1 = new StoreIdImpl("store1");
        final StoreId storeId2 = new StoreIdImpl("store2");
        final long flushPeriod = testConfig.getDirtyMetadataFlushPeriod();
        
        final TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, scheduler, testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        // The background flush should be scheduled
        assertEquals( 1, scheduler.numRemainingTasks() );
        
        final Bucket bucket1 = new StubBucket(storeId1, getFakeBucketMetadata(100, false) );
        final Bucket bucket2 = new StubBucket(storeId2, getFakeBucketMetadata(200, false) );
        
        bucketStoreManager.markBucketAsDirty(bucket1);
        bucketStoreManager.markBucketAsDirty(bucket2);
        
        // Deleting a bucket should remove it from the scheduled set but that could happen while the update is already
        // starting
        bucket1.deleted();
        
        // A bucket close happens when the store is closing (probably for shutdown). This shouldn't block the
        // update from happening
        bucket2.close();
        
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );

        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        UpdatedBucketData expectedUpdate1 = new UpdatedBucketData(storeId2, bucket2.getMetadata());
        
        assertEquals( Sets.newHashSet( expectedUpdate1 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        // Second run should do nothing
        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate1 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        assertEquals( 0, scheduler.numRemainingTasks() );
    }
    
    @Test
    public void testFailOnUpdateBucketMetadata() throws SeqStoreDatabaseException {
        final DeterministicScheduledExecutorService executorService = new DeterministicScheduledExecutorService();
        final Scheduler scheduler = new Scheduler("FakeSchedulerThread", executorService );
        final TestThreadFactory testThreadFactory = new TestThreadFactory();
        final StoreId storeId1 = new StoreIdImpl("store1");
        final StoreId storeId2 = new StoreIdImpl("store2");
        final long flushPeriod = testConfig.getDirtyMetadataFlushPeriod();
        
        final boolean failUpdate[] = new boolean[] { true };
        
        final TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean updateBucketMetadata(StoreId destinationId, BucketPersistentMetaData metadata)
                throws SeqStoreDatabaseException
            {
                if( failUpdate[0] ) {
                    throw new SeqStoreDatabaseException("Test failure");
                }
                return super.updateBucketMetadata(destinationId, metadata);
            }  
        };
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, scheduler, testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        // The background flush should be scheduled
        assertEquals( 1, scheduler.numRemainingTasks() );
        
        final Bucket bucket1 = new StubBucket(storeId1, getFakeBucketMetadata(100, false) );
        final Bucket bucket2 = new StubBucket(storeId2, getFakeBucketMetadata(200, false) );
        
        bucketStoreManager.markBucketAsDirty(bucket1);
        bucketStoreManager.markBucketAsDirty(bucket2);
        
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );

        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        // Both updates tests should have failed but still be scheduled
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );
        
        failUpdate[0] = false;
        
        // Both updates should happen
        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        UpdatedBucketData expectedUpdate1 = new UpdatedBucketData(storeId1, bucket1.getMetadata());
        UpdatedBucketData expectedUpdate2 = new UpdatedBucketData(storeId2, bucket2.getMetadata());
        
        assertEquals( 2, persistenceManager.getUpdatedMetadata().size() );
        assertEquals( Sets.newHashSet( expectedUpdate1, expectedUpdate2 ), 
                      Sets.newHashSet( persistenceManager.getUpdatedMetadata() ) );
        
        // Second run should do nothing
        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        assertEquals( 2, persistenceManager.getUpdatedMetadata().size() );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        assertEquals( 0, scheduler.numRemainingTasks() );
    }
    
    @Test
    public void testUnrecoverableFailOnUpdateBucketMetadata() throws SeqStoreDatabaseException {
        final DeterministicScheduledExecutorService executorService = new DeterministicScheduledExecutorService();
        final Scheduler scheduler = new Scheduler("FakeSchedulerThread", executorService );
        final TestThreadFactory testThreadFactory = new TestThreadFactory();
        final StoreId storeId1 = new StoreIdImpl("store1");
        final StoreId storeId2 = new StoreIdImpl("store2");
        final long flushPeriod = testConfig.getDirtyMetadataFlushPeriod();
        
        final TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public boolean updateBucketMetadata(StoreId destinationId, BucketPersistentMetaData metadata)
                throws SeqStoreDatabaseException
            {
                throw new SeqStoreUnrecoverableDatabaseException( "Failed", "Failure test" );
            }  
        };
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, scheduler, testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        // The background flush should be scheduled
        assertEquals( 1, scheduler.numRemainingTasks() );
        
        final Bucket bucket1 = new StubBucket(storeId1, getFakeBucketMetadata(100, false) );
        final Bucket bucket2 = new StubBucket(storeId2, getFakeBucketMetadata(200, false) );
        
        bucketStoreManager.markBucketAsDirty(bucket1);
        bucketStoreManager.markBucketAsDirty(bucket2);
        
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );

        executorService.tick(flushPeriod, TimeUnit.MILLISECONDS);
        
        // The task should have cancelled itself
        assertEquals( 0, scheduler.numRemainingTasks() );
        
        // Nothing could have been updated
        assertTrue( persistenceManager.getUpdatedMetadata().isEmpty() );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
        assertEquals( 0, scheduler.numRemainingTasks() );
    }
    
    @Test
    public void testMetrics() throws Exception {
        final StoreId storeId1 = new StoreIdImpl("testStore1");
        final StoreId storeId2 = new StoreIdImpl("testStore2");
        
        final List<BucketPersistentMetaData> deletedSharedBucketMetadata = 
                getFakeBucketMetadata( 1000, 2 + testConfig.getNumSharedDBDeletionThreads(), BucketStorageType.SharedDatabase, true );
        final List<BucketPersistentMetaData> deletedDedicatedBucketMetadata = 
                getFakeBucketMetadata( 100000, 2 + testConfig.getNumDedicatedDBDeletionThreads(), BucketStorageType.DedicatedDatabase, true );
        final List<BucketPersistentMetaData> activeSharedBucketMetadata = 
                getFakeBucketMetadata( 200000+6+bucketSeperatorTime, 3, BucketStorageType.SharedDatabase, false);
        final List<BucketPersistentMetaData> activeDedicatedBucketMetadata = 
                getFakeBucketMetadata( 300000+6+bucketSeperatorTime, 3, BucketStorageType.DedicatedDatabase, false);
        
        final List<BucketPersistentMetaData> allBucketMetadata = Lists.newArrayList();
        allBucketMetadata.addAll( deletedSharedBucketMetadata );
        allBucketMetadata.addAll( deletedDedicatedBucketMetadata );
        allBucketMetadata.addAll( activeSharedBucketMetadata );
        allBucketMetadata.addAll( activeDedicatedBucketMetadata );
        
        TestStubStorePersistenceManager persistenceManager = new TestStubStorePersistenceManager() {
            @Override
            public java.util.Collection<BucketPersistentMetaData> getBucketMetadataForStore(StoreId destinationId) 
                    throws SeqStoreDatabaseException 
            {
                assertEquals( storeId1, destinationId );
                return allBucketMetadata;
            }
            
            @Override
            public int getNumBuckets() throws SeqStoreDatabaseException {
                return allBucketMetadata.size();
            }
            
            @Override
            public int getNumDedicatedBuckets() throws SeqStoreDatabaseException {
                return deletedDedicatedBucketMetadata.size() + activeDedicatedBucketMetadata.size();
            }
            
            @Override
            public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(
                StoreId storeIdParam) throws SeqStoreDatabaseException, IllegalStateException
            {
                assertEquals( storeId1, storeIdParam );
                return allBucketMetadata;
            }
            
            @Override
            public boolean deleteBucketStore(StoreId storeId, AckIdV3 bucketId)
                throws SeqStoreDatabaseException, IllegalStateException
            {
                DeletedBucketData data = new DeletedBucketData(storeId, bucketId);
                boolean retval = !deletedBuckets.contains( data );
                deletedBuckets.add(data);
                return retval;
            }
        };
        
        TestThreadFactory testThreadFactory = new TestThreadFactory();
        
        BucketStorageManager bucketStoreManager = new BucketStorageManager(
                persistenceManager, Scheduler.getGlobalInstance(), testThreadFactory);
        bucketStoreManager.setCatchAssertionErrors(false);
        
        assertEquals( deletedDedicatedBucketMetadata.size() + activeDedicatedBucketMetadata.size(), bucketStoreManager.getNumDedicatedBuckets() );
        assertEquals( allBucketMetadata.size(), bucketStoreManager.getNumBuckets() );
        
        // open the store which stores the deletes of all of the buckets marked as deleted
        ConcurrentNavigableMap<AckIdV3, Bucket> buckets = bucketStoreManager.getBucketsForStore(storeId1);
        checkBuckets( allBucketMetadata, buckets );
        
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        assertEquals( activeDedicatedBucketMetadata.size(), bucketStoreManager.getNumDedicatedBuckets() );
        assertEquals( activeDedicatedBucketMetadata.size() + activeSharedBucketMetadata.size(), bucketStoreManager.getNumBuckets() );
        
        // Add 3 unrelated buckets
        bucketStoreManager.createBucket(storeId2, new AckIdV3( 1000, false ), BucketStorageType.DedicatedDatabase );
        bucketStoreManager.createBucket(storeId2, new AckIdV3( 2000, false ), BucketStorageType.SharedDatabase );
        bucketStoreManager.createBucket(storeId2, new AckIdV3( 3000, false ), BucketStorageType.SharedDatabase );
        
        assertEquals( activeDedicatedBucketMetadata.size() + 1, bucketStoreManager.getNumDedicatedBuckets() );
        assertEquals( activeDedicatedBucketMetadata.size() + activeSharedBucketMetadata.size() + 3, bucketStoreManager.getNumBuckets() );
        
        // Delete the store
        bucketStoreManager.deleteBucketsForStore(storeId1, null);
        bucketStoreManager.waitForDeletesToFinish(1, TimeUnit.SECONDS);
        
        assertEquals( 1, bucketStoreManager.getNumDedicatedBuckets() );
        assertEquals( 3, bucketStoreManager.getNumBuckets() );
        
        bucketStoreManager.shutdown();
        
        for( Thread testThread : testThreadFactory.getCreatedThreads() ) {
            assertFalse( testThread.isAlive() ); 
        }
        
        testThreadFactory.rethrow();
    }
}
