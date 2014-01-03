package com.amazon.messaging.seqstore.v3.mapPersistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDeleteInProgressException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreChildManager;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreDeletionTestBase;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Scheduler;


public class StoreDeletionTest extends StoreDeletionTestBase {

    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> createManager(
           BasicConfigProvider<BasicInflightInfo> configProvider,
           Clock clock, boolean truncate)
        throws IOException, InvalidConfigException, SeqStoreException 
    {
        return new MapStoreManager<BasicInflightInfo>(configProvider, clock);
    }
    
    private static void testRemoveWithMarkAllBucketsFailure(
        StorePersistenceManager persistenceManager, final AtomicInteger callCount, int expectedAttempts)
        throws Exception
    {
        TestExecutorService executorService = new TestExecutorService("testRemoveFailure");
        
        final BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        final StoreId storeId = new StoreIdImpl("StoreId");
        final String readerId = "reader";
        
        final SeqStoreManagerV3<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(
                configProvider, persistenceManager, new AlwaysIncreasingClock() );
        
        try {
            // Ensure that the message isn't removed in time there is no reader
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setGuaranteedRetentionPeriod(10000); 
            storeConfig.setMaxMessageLifeTime(-1);
            configProvider.putStoreConfig(storeId.getGroupName(), storeConfig);
            
            manager.createStore(storeId);
            final SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
            final SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
            assertEquals( Collections.singleton( readerId ), store.getReaderNames() );
            
            TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
            store.enqueue(msgIn1, -1, null);
            
            assertNotNull( reader.dequeue() );
            
            manager.deleteStore(storeId);
            
            try {
                store.getReaderNames();
                fail( "Operation on removed store did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            assertNull( manager.getStore( storeId ) );
            assertFalse( manager.containsStore( storeId ) );
            
            manager.waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            assertEquals( expectedAttempts, callCount.get() );
            
            try {
                manager.createStore(storeId);
                fail( "Allowed to create store that wasn't fully deleted.");
            } catch( SeqStoreDeleteInProgressException e ) {
                // Success
            }
            
            executorService.shutdownWithin(100, TimeUnit.MILLISECONDS);
            executorService.rethrow();
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testMarkAllBucketsFailure() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        
        StorePersistenceManager persistenceManager = new NonPersistentBucketCreator<BasicInflightInfo>() {
            @Override
            public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(
                StoreId storeId) throws SeqStoreDatabaseException, IllegalStateException
            {
                callCount.incrementAndGet();
                throw new SeqStoreDatabaseException("Testing failure");
            }
        };
        
        testRemoveWithMarkAllBucketsFailure(persistenceManager, callCount, SeqStoreChildManager.MAX_STORE_DELETION_ATTEMPTS);
    }
    
    @Test
    public void testMarkAllBucketsRuntimeExceptionFailure() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        
        StorePersistenceManager persistenceManager = new NonPersistentBucketCreator<BasicInflightInfo>() {
            @Override
            public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(
                StoreId storeId) throws SeqStoreDatabaseException, IllegalStateException
            {
                callCount.incrementAndGet();
                throw new RuntimeException();
            }
        };
        
        testRemoveWithMarkAllBucketsFailure(persistenceManager, callCount, SeqStoreChildManager.MAX_STORE_DELETION_ATTEMPTS);
    }
    
    @Test
    public void testMarkAllBucketsAssertionFailure() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        
        StorePersistenceManager persistenceManager = new NonPersistentBucketCreator<BasicInflightInfo>() {
            @Override
            public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(
                StoreId storeId) throws SeqStoreDatabaseException, IllegalStateException
            {
                callCount.incrementAndGet();
                throw new AssertionError( "testing" );
            }
        };
        
        testRemoveWithMarkAllBucketsFailure(persistenceManager, callCount, SeqStoreChildManager.MAX_STORE_DELETION_ATTEMPTS);
    }
    
    @Test
    public void testMarkAllBucketsUnrecoverableFailure() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        
        StorePersistenceManager persistenceManager = new NonPersistentBucketCreator<BasicInflightInfo>() {
            @Override
            public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(
                StoreId storeId) throws SeqStoreDatabaseException, IllegalStateException
            {
                callCount.incrementAndGet();
                throw new SeqStoreUnrecoverableDatabaseException("Testing failure", "Testing failure");
            }
        };
        
        testRemoveWithMarkAllBucketsFailure(persistenceManager, callCount, 1);
    }
    
    @Test
    public void testStartDeleteFailure() throws SeqStoreException, InterruptedException {
        StorePersistenceManager persistenceManager = new NonPersistentBucketCreator<BasicInflightInfo>() {
            @Override
            public boolean storeDeletionStarted(StoreId storeId) throws SeqStoreDatabaseException {
                throw new SeqStoreDatabaseException("Starting failure");
            }
        };
        
        TestExecutorService executorService = new TestExecutorService("testRemoveFailure");
        
        final BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        final StoreId storeId = new StoreIdImpl("StoreId");
        final String readerId = "reader";
        
        final SeqStoreManager<AckIdV3, BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(
                configProvider, persistenceManager, new AlwaysIncreasingClock() );
        
        try {
            // Ensure that the message isn't removed in time there is no reader
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setGuaranteedRetentionPeriod(10000); 
            storeConfig.setMaxMessageLifeTime(-1);
            configProvider.putStoreConfig(storeId.getGroupName(), storeConfig);
            
            manager.createStore(storeId);
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
            assertEquals( Collections.singleton( readerId ), store.getReaderNames() );
            
            TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
            store.enqueue(msgIn1, -1, null);
            
            assertNotNull( reader.dequeue() );
            
            try {
                manager.deleteStore(storeId);
                fail( "Didn't fail even though delete didn't work");
            } catch( SeqStoreDatabaseException e ) {
                // Success
            }
            
            // Even if the delete failed it should still be useable
            store = manager.createStore(storeId);
            assertNotNull( store );
            reader = store.createReader(readerId);
            assertNotNull( reader );
            
            TestEntry msgIn2 = new TestEntry("This is a Test Message", "msg1");
            store.enqueue(msgIn2, -1, null);
            
            executorService.shutdownWithin(100, TimeUnit.MILLISECONDS);
            executorService.rethrow();
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testDeleteBeforeMarkHasFinished() throws SeqStoreException, InterruptedException, BrokenBarrierException, TimeoutException {
        final CyclicBarrier markBucketBarrier = new CyclicBarrier(2);
        final CountDownLatch deleteBucketsLatch = new CountDownLatch(1);
        
        StorePersistenceManager persistenceManager = new NonPersistentBucketCreator<BasicInflightInfo>() {
            @Override
            public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(
                StoreId storeId) throws SeqStoreDatabaseException, IllegalStateException
            {
                try {
                    markBucketBarrier.await(1, TimeUnit.SECONDS); // Wait for the main thread to be ready to ready to try the recreate
                    markBucketBarrier.await(1, TimeUnit.SECONDS); // Wait for the main thread to have checked the recreate
                } catch (InterruptedException e) {
                    throw new RuntimeException( "Interrupted", e );
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException( e );
                } catch (TimeoutException e) {
                    throw new SeqStoreDatabaseException(e);
                }
                return super.markAllBucketsForStoreAsPendingDeletion(storeId);
            }
            
            @Override
            public boolean deleteBucketStore(StoreId storeId, AckIdV3 bucketId)
                throws SeqStoreDatabaseException, IllegalStateException
            {
                try {
                    if( !deleteBucketsLatch.await(5, TimeUnit.SECONDS) ) {
                        throw new SeqStoreDatabaseException("Timedout");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException( "Interrupted", e );
                }
                return super.deleteBucketStore(storeId, bucketId);
            }
        };
        
        TestExecutorService executorService = new TestExecutorService("testRemoveFailure");
        
        final BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        final StoreId storeId = new StoreIdImpl("StoreId");
        final String readerId = "reader";
        
        Scheduler scheduler = new Scheduler("Test SeqStoreManager scheduler", 5);
        
        final SeqStoreManagerV3<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(
                configProvider, persistenceManager, scheduler, new AlwaysIncreasingClock() );
        
        try {
            // Ensure that the message isn't removed in time there is no reader
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setGuaranteedRetentionPeriod(10000); 
            storeConfig.setMaxMessageLifeTime(-1);
            configProvider.putStoreConfig(storeId.getGroupName(), storeConfig);
            
            manager.createStore(storeId);
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
            assertEquals( Collections.singleton( readerId ), store.getReaderNames() );
            
            TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
            store.enqueue(msgIn1, -1, null);
            
            assertNotNull( reader.dequeue() );
            
            int numScheduledTasks = scheduler.numRemainingTasks();
            
            manager.deleteStore(storeId);
            assertNull( manager.getStore(storeId) );
            
            // check that the cleanup task got stopped
            assertEquals( numScheduledTasks - 1, scheduler.numRemainingTasks() );
            
            markBucketBarrier.await(1, TimeUnit.SECONDS);
            
            numScheduledTasks = scheduler.numRemainingTasks();
            try {
                manager.createStore(storeId);
                fail( "Recreate was allowed while deletion was still in progress");
            } catch( SeqStoreDeleteInProgressException e ) {
                // Success
            }
            
            assertNull( manager.getStore(storeId) );
            assertEquals( numScheduledTasks, scheduler.numRemainingTasks() );
            
            markBucketBarrier.await(1, TimeUnit.SECONDS);
            Thread.sleep(150); // Allow the mark to finish
            
            // The bucket hasn't been deleted so the store shouldn't be recreated
            try {
                manager.createStore(storeId);
                fail( "Recreate was allowed while deletion was still in progress");
            } catch( SeqStoreDeleteInProgressException e ) {
                // Success
            }
            
            deleteBucketsLatch.countDown();
            manager.waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            
            store = manager.createStore(storeId);
            assertNotNull( store );
            
            reader = store.createReader(readerId);
            assertNotNull( reader );
            
            TestEntry msgIn2 = new TestEntry("This is a Test Message", "msg1");
            store.enqueue(msgIn2, -1, null);
            
            executorService.shutdownWithin(100, TimeUnit.MILLISECONDS);
            executorService.rethrow();
        } finally {
            manager.close();
        }
    }
}
