package com.amazon.messaging.seqstore.v3.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreMetrics;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.SeqStoreReaderMetrics;
import com.amazon.messaging.seqstore.v3.StoredCount;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.bdb.BDBPersistentManager;
import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreIllegalStateException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;
import com.amazon.messaging.utils.Scheduler;

public abstract class BasicSeqStoreTestBase extends TestCase {

    protected SeqStoreManagerV3<BasicInflightInfo> manager;

    protected BasicConfigProvider<BasicInflightInfo> configProvider;
    
    protected Scheduler scheduler;

    protected final SettableClock clock_ = new SettableClock();

    protected final StoreIdImpl storeId = new StoreIdImpl("testStore");

    protected static final String readerId = "testReader";

    public BasicSeqStoreTestBase() {
        super();
    }

    @Before
    public abstract void setUp() throws SeqStoreException, IOException;
    
    @After
    public abstract void tearDown() throws SeqStoreIllegalStateException, SeqStoreException;

    private static Entry createEntry(final StoreIdImpl destinationId, int index, long availableTime) {
        // add leading 0 to keep ordering right
        final String payload = destinationId + "-LogId" + new Formatter().format("%08d", index).toString();
        return new TestEntry(payload.getBytes(), availableTime, null);
    }

    @Test
    public void testBasicEnqueueDequeue() throws SeqStoreException, InterruptedException {
        // Settings for the test
        final long count = 1000;
        final int nackCount = 10;
        final int firstBucketSize = 250;
        final int bucketIntervalSeconds = 1;
        final int bucketIntervalMS = bucketIntervalSeconds * 1000;
        final long firstBucketTime = 1;
        final long secondBucketTime = 2 * bucketIntervalMS + 1;

        final StoreIdImpl destinationId = new StoreIdImpl("destination", "1");
        final String readerId1 = "reader1";
        final String readerId2 = "reader2";
        
        // Assert that the settings make sense
        assertTrue( firstBucketSize < count );
        assertTrue( nackCount < firstBucketSize );
        
        // Set the clock to a known time
        clock_.setCurrentTime(1);
        
        SeqStoreConfig storeConfig = getConfig(bucketIntervalSeconds);
        configProvider.putStoreConfig("destination", storeConfig);

        // Create store and readers
        manager.createStore(destinationId);
        final SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(destinationId);
        store.createReader(readerId1);
        store.createReader(readerId2);
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader1 = store.getReader(readerId1);
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader2 = store.getReader(readerId2);

        // Check that the store is empty
        assertEquals(0l, reader1.getStoreBacklogMetrics().getQueueDepth());
        assertEquals(0l, reader2.getStoreBacklogMetrics().getQueueDepth());
        assertEquals(InflightMetrics.ZeroInFlight, reader1.getInflightMetrics());
        assertEquals(InflightMetrics.ZeroInFlight, reader2.getInflightMetrics());

        long storeSize = 0;
        long storeCount = 0;
        long firstBucketStoreSize = 0;
        long secondBucketStoreSize = 0;
        
        // Enqueue into the first bucket
        for (int i = 0; i < firstBucketSize; i++) {
            Entry entry = createEntry(destinationId, i, firstBucketTime);
            store.enqueue(entry, -1, null);
            storeSize += entry.getPayloadSize() + AckIdV3.LENGTH;
            firstBucketStoreSize += entry.getPayloadSize() + AckIdV3.LENGTH;
            storeCount++;
        }

        // Enqueue into a second bucket
        for (int i = firstBucketSize; i < count; i++) {
            Entry entry = createEntry(destinationId, i, secondBucketTime);
            store.enqueue(entry, -1, null);
            storeSize += entry.getPayloadSize() + AckIdV3.LENGTH;
            secondBucketStoreSize += entry.getPayloadSize() + AckIdV3.LENGTH;
            storeCount++;
        }
        
        store.runCleanupNow();

        SeqStoreMetrics expectedStoreMetrics = new SeqStoreMetrics( 
                new StoredCount(storeCount, storeSize, 2), 
                new StoredCount(storeCount - firstBucketSize, secondBucketStoreSize, 1), 
                0,
                0.0,
                0.0);
                
        // Assert that the store metrics match the enqueues that were done
        assertEquals( expectedStoreMetrics, store.getStoreMetrics());

        // Make sure all messages are available
        clock_.setCurrentTime(3 * bucketIntervalMS);
        
        expectedStoreMetrics = new SeqStoreMetrics( 
                new StoredCount(storeCount, storeSize, 2), 
                new StoredCount( 0, 0, 0 ), 
                clock_.getCurrentTime() - firstBucketTime,
                0.0,
                0.0);

        // Assert that the readers see all messages and don't think that anything is inflight
        assertEquals(count, reader1.getStoreBacklogMetrics().getQueueDepth());
        assertEquals(count, reader2.getStoreBacklogMetrics().getQueueDepth());
        assertEquals(InflightMetrics.ZeroInFlight, reader1.getInflightMetrics());
        assertEquals(InflightMetrics.ZeroInFlight, reader2.getInflightMetrics());

        // Dequeue nackCount messages and make sure that the readers stay insync
        List<InflightEntry<AckIdV3, BasicInflightInfo>> nackMessages = 
                new ArrayList<InflightEntry<AckIdV3, BasicInflightInfo>>(nackCount);
        for (int i = 0; i < nackCount; i++) {
            InflightEntry<AckIdV3, BasicInflightInfo> e1 = reader1.dequeue();
            InflightEntry<AckIdV3, BasicInflightInfo> e2 = reader2.dequeue();
            assertEquals(e1.getAckId(), e2.getAckId());
            nackMessages.add(e1);
        }
        
        SeqStoreReaderMetrics expectedReaderMetrics = new SeqStoreReaderMetrics(
                new StoredCount( count, storeSize, 2 ),
                new StoredCount( nackCount, firstBucketStoreSize, 1 ),
                new StoredCount( count - nackCount, storeSize, 2 ),
                StoredCount.EmptyStoredCount,
                clock_.getCurrentTime() - firstBucketTime );

        // Assert that the metrics record the dequeues
        assertEquals(expectedReaderMetrics, reader1.getStoreBacklogMetrics());
        assertEquals(expectedReaderMetrics, reader2.getStoreBacklogMetrics());
        assertEquals( new InflightMetrics( nackCount, 0 ), reader1.getInflightMetrics());
        assertEquals( new InflightMetrics( nackCount, 0 ), reader2.getInflightMetrics());

        // schedule all dequeued messages to be re-deliverable immediately
        for (InflightEntry<AckIdV3, BasicInflightInfo> entry : nackMessages) {
            assertTrue( nack( reader1, entry ) );
            assertTrue( nack( reader2, entry ) );
        }
        
        store.runCleanupNow();

        // Assert that the store hasn't lost any messages
        assertEquals( expectedStoreMetrics, store.getStoreMetrics());
        
        // Assert that the nacked messages show up in the metrics
        assertEquals(expectedReaderMetrics, reader1.getStoreBacklogMetrics());
        assertEquals(expectedReaderMetrics, reader2.getStoreBacklogMetrics());
        assertEquals( new InflightMetrics(0, nackCount), reader1.getInflightMetrics());
        assertEquals( new InflightMetrics(0, nackCount), reader2.getInflightMetrics());

        // Get all messages from the first bucket + 1 to move the ack level out of the bucket
        List<AckIdV3> reader1Acked = new ArrayList<AckIdV3>();
        for (int i = 0; i < firstBucketSize + 1; i++) {
            StoredEntry<AckIdV3> e1 = reader1.dequeue();
            reader1.ack(e1.getAckId());
            reader1Acked.add(e1.getAckId());
        }
        
        store.runCleanupNow();
        
        // Asert the store hasn't dropped anything yet - it can't do so because reader2 is still in the 1st bucket.
        assertEquals( expectedStoreMetrics, store.getStoreMetrics());
        
        SeqStoreReaderMetrics expectedReader1Metrics = new SeqStoreReaderMetrics(
                new StoredCount( count - ( firstBucketSize + 1 ), secondBucketStoreSize, 1 ),
                new StoredCount( 0, 0, 0 ),
                new StoredCount( count - ( firstBucketSize + 1 ), secondBucketStoreSize, 1 ),
                StoredCount.EmptyStoredCount,
                clock_.getCurrentTime() - secondBucketTime );

        // Assert the dequeues have been recorded
        assertEquals( expectedReader1Metrics, reader1.getStoreBacklogMetrics() );
        assertEquals( expectedReaderMetrics, reader2.getStoreBacklogMetrics() );
        assertEquals( InflightMetrics.ZeroInFlight, reader1.getInflightMetrics() );
        assertEquals( new InflightMetrics(0, 10), reader2.getInflightMetrics() );

        // Get and ack the messages acked using reader1 using reader2
        for (int i = 0; i < firstBucketSize + 1; i++) {
            StoredEntry<AckIdV3> e2 = reader2.dequeue();
            assertEquals(reader1Acked.get(i), e2.getAckId());
            reader2.ack(e2.getAckId());
        }

        // Force cleanup which should drop the first bucket
        store.runCleanupNow();
        
        // Assert the first bucket has been dropped
        expectedStoreMetrics = new SeqStoreMetrics( 
                new StoredCount(storeCount - firstBucketSize, storeSize - firstBucketStoreSize, 1 ), 
                new StoredCount( 0, 0, 0 ), 
                clock_.getCurrentTime() - secondBucketTime,
                0.0,
                0.0);
        assertEquals( expectedStoreMetrics, store.getStoreMetrics());

        // Ack all remaining messages
        for (int i = firstBucketSize + 1; i < count; i++) {
            StoredEntry<AckIdV3> e1 = reader1.dequeue();
            StoredEntry<AckIdV3> e2 = reader2.dequeue();
            assertEquals(e1.getAckId(), e2.getAckId());
            reader1.ack(e1.getAckId());
            reader2.ack(e2.getAckId());
        }
        
        // Assert everything is empty
        expectedReaderMetrics = new SeqStoreReaderMetrics(
                StoredCount.EmptyStoredCount,
                StoredCount.EmptyStoredCount,
                StoredCount.EmptyStoredCount,
                StoredCount.EmptyStoredCount,
                0 );
        
        assertEquals(0l, reader1.getStoreBacklogMetrics().getQueueDepth());
        assertEquals(0l, reader2.getStoreBacklogMetrics().getQueueDepth());
        assertEquals(InflightMetrics.ZeroInFlight, reader1.getInflightMetrics());
        assertEquals(InflightMetrics.ZeroInFlight, reader2.getInflightMetrics());

        // Cleanup
        store.removeReader(readerId1);
        store.removeReader(readerId2);
    }

    private static SeqStoreConfig getConfig(final int bucketIntervalSeconds) {
        BucketStoreConfig bucketConfiguration = 
                new BucketStoreConfig.Builder()
                    .withMinPeriod(bucketIntervalSeconds)
                    .withMaxEnqueueWindow(0)
                    .withMaxPeriod(Integer.MAX_VALUE)
                    .withMinSize(Integer.MAX_VALUE)
                    .build();

        // Ensure that the store config is what we expect it to be
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setDedicatedDBBucketStoreConfig(bucketConfiguration);
        storeConfig.setSharedDBBucketStoreConfig(bucketConfiguration);
        storeConfig.setMaxSizeInKBBeforeUsingDedicatedDBBuckets(1);
        storeConfig.setGuaranteedRetentionPeriod(0);
        storeConfig.setMaxMessageLifeTime(-1);
        return storeConfig;
    }
    
    // Test that the dequeueFromStore and dequeueFromInflight work correctly.
    @Test
    public void testSplitStoreInflightDequeue() throws SeqStoreException, InterruptedException {
        final StoreIdImpl destinationId = new StoreIdImpl("destination", "2");
        final int redriveTime = 100;
        
        SeqStoreReaderConfig<BasicInflightInfo> readerConfig = 
            new SeqStoreReaderConfig<BasicInflightInfo>( new BasicInflightInfoFactory(redriveTime) );
        
        configProvider.putReaderConfig(destinationId.getGroupName(), readerId, readerConfig);
            
        manager.createStore(destinationId);
        final SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(destinationId);
        
        store.createReader(readerId);
        
        SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = store.getReader(readerId);
        
        final long message1Time = 5;
        final long message2Time = 10;
        final long message3Time = message1Time + redriveTime + 1;
        
        // Enqueue
        Entry e1 = createEntry(destinationId, 1, message1Time);
        store.enqueue(e1, -1, null);
        Entry e2 = createEntry(destinationId, 2, message2Time);
        store.enqueue(e2, -1, null);
        Entry e3 = createEntry(destinationId, 3, message3Time);
        store.enqueue(e3, -1, null);
        
        assertEquals( message1Time, reader.getTimeOfNextMessage() );
        assertEquals( Long.MAX_VALUE, reader.getTimeOfNextInflightMessage() );
        assertEquals( message1Time, reader.getTimeOfNextStoreMessage() );
        
        clock_.setCurrentTime(message1Time);
        
        assertNull( reader.dequeueFromInflight() );
        
        StoredEntry<AckIdV3> d1 = reader.dequeueFromStore();
        assertNotNull( d1 );
        assertEquals( e1.getLogId(), d1.getLogId() );
        long redeliveryTime = message1Time + redriveTime;
        
        assertEquals( message2Time, reader.getTimeOfNextMessage() );
        assertEquals( redeliveryTime, reader.getTimeOfNextInflightMessage() );
        assertEquals( message2Time, reader.getTimeOfNextStoreMessage() );
        
        // Nothing is available from inflight yet
        assertNull( reader.dequeueFromInflight() );
        
        clock_.setCurrentTime(redeliveryTime);
        
        InflightEntry<AckIdV3, BasicInflightInfo> d2 = reader.dequeueFromInflight();
        assertNotNull( d2 );
        assertEquals( e1.getLogId(), d2.getLogId() );
        long newRedeliveryTime = redeliveryTime + redriveTime;
        
        assertEquals( message2Time, reader.getTimeOfNextMessage() );
        assertEquals( newRedeliveryTime, reader.getTimeOfNextInflightMessage() );
        assertEquals( message2Time, reader.getTimeOfNextStoreMessage() );
        
        // Nack the message
        assertTrue( nack( reader, d2 ) );
        long redeliveryTimeAfterNack = redeliveryTime;
        
        assertEquals( message2Time, reader.getTimeOfNextMessage() );
        assertEquals( redeliveryTimeAfterNack, reader.getTimeOfNextInflightMessage() );
        assertEquals( message2Time, reader.getTimeOfNextStoreMessage() );
        
        StoredEntry<AckIdV3> d3 = reader.dequeueFromStore();
        assertNotNull( d3 );
        assertEquals( e2.getLogId(), d3.getLogId() );
        
        assertEquals( redeliveryTimeAfterNack, reader.getTimeOfNextMessage() );
        assertEquals( redeliveryTimeAfterNack, reader.getTimeOfNextInflightMessage() );
        assertEquals( message3Time, reader.getTimeOfNextStoreMessage() );
    }
    
    @Test
    public void testAckLevels() throws SeqStoreException, InterruptedException {
        final StoreIdImpl destinationId = new StoreIdImpl("destination", "2");
        
        manager.createStore(destinationId);
        final SeqStore<AckIdV3, BasicInflightInfo> store2 = manager.getStore(destinationId);
        
        store2.createReader(readerId);
        
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store2.getReader(readerId);
        
        final int count = 500;
        
        // Enqueue
        for (int i = 0; i < count; i++) {
            Entry e2 = createEntry(destinationId, i, 1);
            store2.enqueue(e2, -1, null);
        }
        
        // 5 in flight
        AckIdV3 ackLevel = new AckIdV3(0, 0, 0);
        // get the same store and check if things stay in sync accross
        // references.
        SeqStore<AckIdV3, BasicInflightInfo> store22 = manager.getStore(destinationId);
        TopDisposableReader<BasicInflightInfo> readerPersistence21 = (TopDisposableReader<BasicInflightInfo>) store22.getReader(readerId);
        for (int i = 0; i < count / 5; i++) {
            StoredEntry<AckIdV3> e1 = reader.dequeue();
            StoredEntry<AckIdV3> e2 = reader.dequeue();
            StoredEntry<AckIdV3> e3 = reader.dequeue();
            StoredEntry<AckIdV3> e4 = reader.dequeue();
            StoredEntry<AckIdV3> e5 = reader.dequeue();
            assertTrue(readerPersistence21.getAckLevel().compareTo(ackLevel) >= 0);
            ackLevel = readerPersistence21.getAckLevel();
            reader.ack(e2.getAckId());
            reader.ack(e5.getAckId());
            reader.ack(e4.getAckId());
            reader.ack(e3.getAckId());
            
            // should not have changed...
            assertEquals(readerPersistence21.getAckLevel(), ackLevel); 
            reader.ack(e1.getAckId());
            ackLevel = e5.getAckId();
        }

        store2.removeReader(readerId);
    }
    
    @Test
    public void testNackThenDequeue() throws SeqStoreException, InterruptedException {
        String destination = storeId.getGroupName();
        String readerName = "reader";
        
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setGuaranteedRetentionPeriod(0);
        storeConfig.setMaxMessageLifeTime(-1);
        configProvider.putStoreConfig(destination, storeConfig);
        
        SeqStoreReaderConfig<BasicInflightInfo> readerConfig = new SeqStoreReaderConfig<BasicInflightInfo>(
                new BasicInflightInfoFactory( 60000 ) );
        configProvider.putReaderConfig(destination, readerName, readerConfig);

        // Create store and readers
        manager.createStore(storeId);
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerName);
        
        Entry entry = createEntry(storeId, 0, 1);
        store.enqueue(entry, -1, null);
        
        InflightEntry<AckIdV3, BasicInflightInfo> dequeued = reader.dequeue();
        assertNotNull( dequeued );
        assertEquals( dequeued.getLogId(), entry.getLogId() );
        
        assertTrue( nack( reader, dequeued ) );
        StoredEntry<AckIdV3> dequeuedAgain = reader.dequeue();
        assertNotNull( dequeuedAgain );
        assertEquals( dequeued.getAckId(), dequeuedAgain.getAckId() );
    }
    
    @Test
    public void testCreateStore() throws SeqStoreException {
        int numScheduled = scheduler.numRemainingTasks();
        
        manager.createStore(storeId);
        
        // Cleanup task should have been scheduled
        assertEquals( 1 + numScheduled, scheduler.numRemainingTasks() );
        
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
        assertEquals(storeId.getStoreName(), store.getStoreName());
        
        // Delete the store
        manager.deleteStore(storeId);
        
        // Cleanup task should have been cancelled
        assertEquals( numScheduled, scheduler.numRemainingTasks() );
        
        manager.close();
        
    }

    @Test
    public void testCreateReader() throws SeqStoreException {
        manager.createStore(storeId);
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
        store.createReader(readerId);
        manager.deleteStore(storeId);
        manager.close();
    }
    
    @Test
    public void testEnqueueDequeue() throws SeqStoreException, InterruptedException {
        manager.createStore(storeId);
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
        TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
        store.enqueue(msgIn1, -1, null);
        StoredEntry<AckIdV3> msgOut1 = reader.dequeue();
        assertEquals(msgIn1.getLogId(), msgOut1.getLogId());
        assertEquals(new String(msgIn1.getPayload()), new String(msgOut1.getPayload()));
        reader.ack(msgOut1.getAckId());
        manager.deleteStore(storeId);
        manager.close();
    }
    
    @Test
    public void testNoReaderEnqueueThenDequeue() throws SeqStoreException, InterruptedException {
        manager.createStore(storeId);
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
        TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
        store.enqueue(msgIn1, -1, null);
        clock_.setCurrentTime(clock_.getCurrentTime() + 1);
        store.runCleanupNow();
        // Thread.sleep(2);
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
        assertNull("Dequeue not Null", reader.dequeue());
        manager.deleteStore(storeId);
        manager.close();
    }
    
    @Test
    public void testDeleteStore() throws SeqStoreException, InterruptedException, TimeoutException {
        final int bucketIntervalSeconds = 5;
        final long bucketIntervalMS = bucketIntervalSeconds * 1000;
        final int numBucketsToCreate = BDBPersistentManager.BUCKETS_TO_MARK_DELETED_PER_TRANSACTION * 2 + 1;
        
        SeqStoreConfig storeConfig = getConfig(bucketIntervalSeconds);
        configProvider.putStoreConfig(storeId.getGroupName(), storeConfig);
        
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore(storeId);
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
        
        long startTime = clock_.getCurrentTime();
        for( int i = 0; i < numBucketsToCreate; ++i ) {
            TestEntry msg = new TestEntry("This is a Test Message:" + i, "msg" + i);
            store.enqueue(msg, -1, null);
            clock_.increaseTime( bucketIntervalMS * 2 );
        }
        
        for( int i = 0; i < numBucketsToCreate; ++i ) {
             InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
             assertNotNull( entry );
             assertEquals( "msg"+ i , entry.getLogId() );
             assertEquals( startTime + ( i * bucketIntervalMS * 2), entry.getAvailableTime() );
        }
        
        assertEquals( numBucketsToCreate, store.getStoreMetrics().getTotalMessageCount().getBucketCount() );
        
        manager.deleteStore(storeId);
        
        // Check that the store is gone
        store = manager.getStore(storeId);
        assertNull( store );
        assertFalse( manager.getStoreNames().contains( storeId ) );
        
        // Test that we can recreate the store after the background deletion is completed
        manager.waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
        
        store = manager.createStore(storeId);
        // The store should not have the old reader
        reader = store.getReader(readerId);
        assertNull( reader );
        assertTrue( store.getReaderNames().isEmpty() );
        
        // Should be able to recreate the reader
        reader = store.createReader(readerId);
        assertNotNull( reader );
        
        // and enqueue and dequeue
        for( int i = numBucketsToCreate; i < 2*numBucketsToCreate; ++i ) {
            TestEntry msg = new TestEntry("This is a Test Message:" + i, "msg" + i);
            store.enqueue(msg, -1, null);
            clock_.increaseTime( bucketIntervalMS * 2 );
        }
        
        for( int i = numBucketsToCreate; i < 2*numBucketsToCreate; ++i ) {
             InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
             assertNotNull( entry );
             assertEquals( "msg"+ i , entry.getLogId() );
             assertEquals( startTime + ( i * bucketIntervalMS * 2), entry.getAvailableTime() );
        }
        
        manager.close();
    }

    private static boolean nack(
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader, 
        InflightEntry<AckIdV3, BasicInflightInfo> entry )
            throws SeqStoreException 
    {
        return reader.update( 
                entry.getAckId(), 
                new InflightUpdateRequest<BasicInflightInfo>()
                    .withNewTimeout(0)
                    .withNewInflightInfo(entry.getInflightInfo().nextDequeueInfo())
                    .withExpectedInflightInfo(entry.getInflightInfo())) == InflightUpdateResult.DONE;
    }
    
}
