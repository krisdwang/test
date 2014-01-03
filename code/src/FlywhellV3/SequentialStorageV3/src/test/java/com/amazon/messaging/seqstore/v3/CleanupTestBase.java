package com.amazon.messaging.seqstore.v3;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;

public abstract class CleanupTestBase extends TestCase {

    protected static final BucketStoreConfig BUCKET_CONFIGURATION = 
            new BucketStoreConfig.Builder()
                .withMinPeriod(2)
                .withMaxEnqueueWindow(0)
                .withMaxPeriod(2)
                .withMinSize(0)
                .build();

    protected static final SeqStoreConfig STORE_CONFIG;

    static {
        STORE_CONFIG = new SeqStoreConfig();
        STORE_CONFIG.setDedicatedDBBucketStoreConfig(BUCKET_CONFIGURATION);
        STORE_CONFIG.setSharedDBBucketStoreConfig(BUCKET_CONFIGURATION);
        // 100mb - shouldn't ever reach this
        STORE_CONFIG.setMaxSizeInKBBeforeUsingDedicatedDBBuckets(100*1024); 
    }

    protected final boolean useLLMEnv;

    public CleanupTestBase(boolean useLLMEnv) {
        this.useLLMEnv = useLLMEnv;
    }

    protected abstract SeqStoreManagerV3<BasicInflightInfo> getManager(
        Clock clock, BasicConfigProvider<BasicInflightInfo> configProvider, boolean truncate)
        throws InvalidConfigException, SeqStoreException, IOException;

    protected long getMaxPeriodMS() {
        return BUCKET_CONFIGURATION.getMaxPeriod() * 1000l;
    }

    /**
     * Should return true if the manager state is persistent across close and
     * recreate.
     * 
     * @return
     */
    protected abstract boolean isManagerStatePersistent();

    private void setupStoreConfig(
        BasicConfigProvider<BasicInflightInfo> configProvider, String storeName, long minRetention,
        long maxRetention)
    {
        setupStoreConfig( configProvider, storeName, 2, minRetention, maxRetention);
    }
    
    private void setupStoreConfig(
        BasicConfigProvider<BasicInflightInfo> configProvider, String storeName,
        int minBucketsToCompress, long minRetention, long maxRetention)
    {
        SeqStoreConfig storeConfig = STORE_CONFIG.clone();
        storeConfig.setGuaranteedRetentionPeriod(minRetention);
        storeConfig.setMaxMessageLifeTime(maxRetention);
        storeConfig.setMinBucketsToCompress(minBucketsToCompress);
        configProvider.putStoreConfig(storeName, storeConfig);
    }

    private void setupReaderConfig(
        BasicConfigProvider<BasicInflightInfo> configProvider, String storeName, String readerName,
        int redeliveryTimeout)
    {
        SeqStoreReaderConfig<BasicInflightInfo> config = new SeqStoreReaderConfig<BasicInflightInfo>(
                new BasicInflightInfoFactory(redeliveryTimeout));
        configProvider.putReaderConfig(storeName, readerName, config);
    }

    @Test
    public void testNoReader()
        throws SeqStoreException, InvalidConfigException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final long bucketPeriod = getMaxPeriodMS();
        final long minRetentionPeriod = bucketPeriod / 2;

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        clock.setCurrentTime(getMaxPeriodMS() * 2);

        setupStoreConfig(configProvider, storeName, minRetentionPeriod, Long.MAX_VALUE);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = (SeqStoreInternalInterface<BasicInflightInfo>) manager.createStore(new StoreIdImpl(
                    storeName));

            store.enqueue(new TestEntry("Entry1", 1), 0, null);

            assertEquals(1, store.getNumBuckets());
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());

            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(
                    new AckIdV3(clock.getCurrentTime() - minRetentionPeriod, false),
                    store.getMinAvailableLevel());

            // The message can't be deleted yet as its still possible for new
            // messages to be added to the bucket.
            clock.increaseTime(minRetentionPeriod + 1);
            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(
                    new AckIdV3(clock.getCurrentTime() - minRetentionPeriod, false),
                    store.getMinAvailableLevel());

            clock.increaseTime(bucketPeriod);
            store.runCleanupNow(); // This should delete all messages

            assertEquals(0, store.getNumBuckets());
            assertEquals(0, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(
                    new AckIdV3(clock.getCurrentTime() - minRetentionPeriod, false),
                    store.getMinAvailableLevel());

            // Add two new messages, one scheduled now, one several buckets in
            // the future.
            store.enqueue(new TestEntry("Entry2", clock.getCurrentTime()), 0, null);
            long delayMessageTime = clock.getCurrentTime() + 2 * bucketPeriod;
            store.enqueue(new TestEntry("Entry3", delayMessageTime), 0, null);

            store.runCleanupNow();

            assertEquals(2, store.getNumBuckets());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(
                    new AckIdV3(clock.getCurrentTime() - minRetentionPeriod, false),
                    store.getMinAvailableLevel());

            clock.increaseTime(minRetentionPeriod - 1);
            store.runCleanupNow(); // Still in the same bucket so no deletion
                                   // should happen

            assertEquals(2, store.getNumBuckets());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(
                    new AckIdV3(clock.getCurrentTime() - minRetentionPeriod, false),
                    store.getMinAvailableLevel());

            clock.increaseTime(bucketPeriod);
            store.runCleanupNow(); // Now the first bucket should be deleted but
                                   // the second should remain

            assertEquals(1, store.getNumBuckets());
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(
                    new AckIdV3(clock.getCurrentTime() - minRetentionPeriod, false),
                    store.getMinAvailableLevel());

            clock.setCurrentTime(delayMessageTime);
            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(
                    new AckIdV3(clock.getCurrentTime() - minRetentionPeriod, false),
                    store.getMinAvailableLevel());

            clock.increaseTime(bucketPeriod);
            store.runCleanupNow();

            assertEquals(0, store.getNumBuckets());
            assertEquals(0, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(
                    new AckIdV3(clock.getCurrentTime() - minRetentionPeriod, false),
                    store.getMinAvailableLevel());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testWithReaders()
        throws SeqStoreException, InvalidConfigException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final String readerId2 = "Reader2";
        final long bucketPeriod = getMaxPeriodMS();

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, 1);
        setupReaderConfig(configProvider, storeName, readerId2, 1);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader1 = store.createReader(readerId1);

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader2 = store.createReader(readerId2);

            store.enqueue(new TestEntry("Entry1", 1), 0, null);
            store.enqueue(new TestEntry("Entry2", 1), 0, null);

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(2, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(2, reader2.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(new AckIdV3(0, false), reader1.getAckLevel());
            assertEquals(new AckIdV3(0, false), reader2.getAckLevel());

            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(2, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(2, reader2.getStoreBacklogMetrics().getQueueDepth());

            InflightEntry<AckIdV3, BasicInflightInfo> entry1 = reader1.dequeue();
            InflightEntry<AckIdV3, BasicInflightInfo> entry2 = reader2.dequeue();

            assertNotNull(entry1);
            assertNotNull(entry2);
            assertEquals(entry1.getAckId(), entry2.getAckId());

            AckIdV3 firstId = entry1.getAckId();

            reader1.ack(firstId);

            entry1 = reader1.dequeue();
            assertNotNull(entry1);

            AckIdV3 secondId = entry1.getAckId();

            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(initialTime, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader2.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(new AckIdV3(secondId, false), reader1.getAckLevel());
            assertEquals(new AckIdV3(firstId, false), reader2.getAckLevel());

            clock.increaseTime(bucketPeriod);
            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(firstId, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader2.getStoreBacklogMetrics().getQueueDepth());

            reader2.ack(firstId); // Should allow cleanedTillLevel to move
            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(firstId, true), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader2.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(new AckIdV3(secondId, false), reader1.getAckLevel());
            assertEquals(new AckIdV3(firstId, true), reader2.getAckLevel());

            reader1.ack(secondId); // Shouldn't change anything for cleanup
            assertNull(reader1.dequeue());

            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(firstId, true), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader2.getStoreBacklogMetrics().getQueueDepth());

            entry2 = reader2.dequeue();
            assertEquals(secondId, entry2.getAckId());
            reader2.ack(secondId);

            // Everything is now acked
            assertEquals(new AckIdV3(secondId, true), reader2.getAckLevel());
            assertNull(reader2.dequeue());

            store.runCleanupNow();
            assertEquals(0, store.getNumBuckets());
            checkMinAvailable(clock, store, secondId);
            assertEquals(0, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader2.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(new AckIdV3(secondId, true), reader1.getAckLevel());
            assertEquals(new AckIdV3(secondId, true), reader2.getAckLevel());
            assertNull(reader1.dequeue());
            assertNull(reader2.dequeue());

            if (isManagerStatePersistent()) {
                // Close and reopen the manager and make sure the ack levels are
                // persistent even if they're not pointing
                // to real messages
                manager.close();
                manager = null;
                store = null;
                reader1 = null;
                reader2 = null;

                manager = getManager(clock, configProvider, false);
                store = manager.getStore(new StoreIdImpl(storeName));
                assertNotNull(store);
                reader1 = store.getReader(readerId1);
                reader2 = store.createReader(readerId2);
                assertNotNull(reader1);
                assertNotNull(reader2);

                assertEquals(0, store.getNumBuckets());
                assertEquals(0, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
                assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
                assertEquals(0, reader2.getStoreBacklogMetrics().getQueueDepth());
                assertEquals(new AckIdV3(secondId, true), reader1.getAckLevel());
                assertEquals(new AckIdV3(secondId, true), reader2.getAckLevel());

                store.runCleanupNow();
                checkMinAvailable(clock, store, secondId);
            }
        } finally {
            if (manager != null)
                manager.close();
        }
    }

    /**
     * Check that min available is at or below the current time but above the specified id
     */
    private void checkMinAvailable(
        SettableClock clock, SeqStoreInternalInterface<BasicInflightInfo> store, AckIdV3 ackLevel)
        throws SeqStoreClosedException
    {
        AckIdV3 minAvailableLevel = store.getMinAvailableLevel();
        AckIdV3 max = new AckIdV3(clock.getCurrentTime() - store.getConfig().getGuaranteedRetentionPeriod(), false);
        assertTrue( minAvailableLevel + " > " + max, max.compareTo( minAvailableLevel ) >= 0 );
        if( ackLevel.compareTo( max ) < 0 ) {
            assertTrue( ackLevel + " > " + minAvailableLevel, ackLevel.compareTo(minAvailableLevel) <= 0 );
        }
    }

    @Test
    public void testWithReadersWithMaxRetention()
        throws SeqStoreException, InvalidConfigException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final String readerId2 = "Reader2";
        final long bucketPeriod = getMaxPeriodMS();

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        long maxRetentionPeriod = bucketPeriod * 4;
        setupStoreConfig(configProvider, storeName, 0, maxRetentionPeriod);
        setupReaderConfig(configProvider, storeName, readerId1, 1);
        setupReaderConfig(configProvider, storeName, readerId2, 1);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader1 = store.createReader(readerId1);

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader2 = store.createReader(readerId2);

            store.enqueue(new TestEntry("Entry1", 1), 0, null);
            store.enqueue(new TestEntry("Entry2", 1), 0, null);

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(2, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(2, reader2.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(new AckIdV3(0, false), reader1.getAckLevel());
            assertEquals(new AckIdV3(0, false), reader2.getAckLevel());

            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(2, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(2, reader2.getStoreBacklogMetrics().getQueueDepth());

            InflightEntry<AckIdV3, BasicInflightInfo> entry1 = reader1.dequeue();
            InflightEntry<AckIdV3, BasicInflightInfo> entry2 = reader2.dequeue();

            assertNotNull(entry1);
            assertNotNull(entry2);
            assertEquals(entry1.getAckId(), entry2.getAckId());

            AckIdV3 firstId = entry1.getAckId();

            reader1.ack(firstId);

            entry1 = reader1.dequeue();
            assertNotNull(entry1);

            AckIdV3 secondId = entry1.getAckId();

            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(initialTime, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader2.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(new AckIdV3(secondId, false), reader1.getAckLevel());
            assertEquals(new AckIdV3(firstId, false), reader2.getAckLevel());

            clock.increaseTime(bucketPeriod);
            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            assertEquals(new AckIdV3(firstId, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader2.getStoreBacklogMetrics().getQueueDepth());

            // Drop all the message off of the disk
            clock.increaseTime(maxRetentionPeriod);
            store.runCleanupNow();

            assertEquals(0, store.getNumBuckets());
            final AckIdV3 maxRetentionLevel = new AckIdV3(clock.getCurrentTime() - maxRetentionPeriod, false);
            assertEquals(maxRetentionLevel, store.getMinAvailableLevel());
            assertEquals(0, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader2.getStoreBacklogMetrics().getQueueDepth());

            assertNull(reader1.dequeue());
            assertNull(reader2.dequeue());
            assertEquals(maxRetentionLevel, reader1.getAckLevel());
            assertEquals(maxRetentionLevel, reader2.getAckLevel());
        } finally {
            if (manager != null)
                manager.close();
        }
    }

    @Test
    public void testWithReadersWithDelay()
        throws SeqStoreException, InvalidConfigException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final long bucketPeriod = getMaxPeriodMS();

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, 1);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = store.createReader(readerId1);

            store.enqueue(new TestEntry("Entry1", initialTime), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 4 * bucketPeriod), 0, null);

            assertEquals(2, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(0, false), reader.getAckLevel());

            store.runCleanupNow();

            assertEquals(2, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());

            // Dequeue and ack the message
            InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
            assertNotNull(entry);
            reader.ack(entry.getAckId());
            assertNull(reader.dequeue());

            store.runCleanupNow();
            assertEquals(2, store.getNumBuckets());
            checkMinAvailable( clock, store, new AckIdV3( entry.getAckId(), true ) );
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());

            clock.increaseTime(bucketPeriod);
            store.runCleanupNow();

            assertEquals(1, store.getNumBuckets());
            checkMinAvailable( clock, store, new AckIdV3( entry.getAckId(), true ) );
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());

            assertNull(reader.dequeue());

            store.enqueue(new TestEntry("Entry3", initialTime + 3 * bucketPeriod), 0, null);
            assertEquals(2, store.getNumBuckets());
            checkMinAvailable( clock, store, new AckIdV3( entry.getAckId(), true ) );
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(2, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());

            clock.setCurrentTime(initialTime + 5 * bucketPeriod);

            assertEquals(2, store.getNumBuckets());
            assertEquals(2, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(2, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());

            // Ensure that no message is skipped because of lastAvailable was
            // temporarily null
            entry = reader.dequeue();
            assertNotNull(entry);
            assertEquals("Entry3", entry.getLogId());
            reader.ack(entry.getAckId());

            entry = reader.dequeue();
            assertNotNull(entry);
            assertEquals("Entry2", entry.getLogId());
            reader.ack(entry.getAckId());
        } finally {
            if (manager != null)
                manager.close();
        }
    }

    @Test
    public void testCleanupRunDuringEnqueue()
        throws SeqStoreException, InvalidConfigException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final long bucketPeriod = getMaxPeriodMS();

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        final long initialTime = bucketPeriod * 102;
        final long maxMsgLifeTime = bucketPeriod * 100;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 0, maxMsgLifeTime);
        setupReaderConfig(configProvider, storeName, readerId1, 1);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = store.createReader(readerId1);

            AckIdV3 ackId = store.getAckIdForEnqueue(initialTime);
            assertEquals(new AckIdV3(ackId, false), store.getMinEnqueueLevel());
            assertEquals(Long.MAX_VALUE, reader.getTimeOfNextStoreMessage());
            clock.increaseTime(1);
            store.runCleanupNow();
            store.enqueue(ackId, new TestEntry("Entry1", initialTime), 0, true, null);
            assertEquals(new AckIdV3(ackId, false), store.getMinEnqueueLevel());
            assertEquals(Long.MAX_VALUE, reader.getTimeOfNextStoreMessage());
            clock.increaseTime(1);
            store.runCleanupNow();
            store.enqueueFinished(ackId);

            clock.increaseTime(10);

            assertTrue(reader.getTimeOfNextStoreMessage() <= initialTime);
            assertEquals(1, store.getNumBuckets());
            assertTrue(ackId.compareTo(store.getMinAvailableLevel()) > 0);
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertTrue(ackId.compareTo(reader.getAckLevel()) > 0);

            store.runCleanupNow();

            assertTrue(reader.getTimeOfNextStoreMessage() <= initialTime);
            assertEquals(1, store.getNumBuckets());
            assertTrue(ackId.compareTo(store.getMinAvailableLevel()) > 0);
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());
            assertTrue(ackId.compareTo(reader.getAckLevel()) > 0);

            // Dequeue and ack the message
            InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
            assertNotNull(entry);
            reader.ack(entry.getAckId());

            store.runCleanupNow();
            assertEquals(1, store.getNumBuckets());
            checkMinAvailable( clock, store, new AckIdV3( entry.getAckId(), true ) );
            assertEquals(1, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());

            clock.increaseTime(bucketPeriod);
            store.runCleanupNow();

            assertEquals(0, store.getNumBuckets());
            checkMinAvailable( clock, store, new AckIdV3( entry.getAckId(), true ) );
            assertEquals(0, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());

            assertNull(reader.dequeue());
        } finally {
            if (manager != null)
                manager.close();
        }
    }
    
    /**
     * Test an edge case where cleanup runs when everything is acked, and the time is after the end of the bucket 
     * which moves the ackid into a new bucket and delete's the old one.
     * 
     * @throws SeqStoreException
     * @throws InvalidConfigException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testCleanupBuckedChangeEdgeCase() throws SeqStoreException, InvalidConfigException, IOException, InterruptedException {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        
        BasicConfigProvider<BasicInflightInfo> configProvider = 
            BasicConfigProvider.newBasicInflightConfigProvider();
        
        SettableClock clock = new SettableClock();
        final long initialTime = getMaxPeriodMS() * 102;
        final long maxMsgLifeTime = -1;
        clock.setCurrentTime( initialTime );
        
        setupStoreConfig( configProvider, storeName, 0, maxMsgLifeTime );
        setupReaderConfig(configProvider, storeName, readerId1, 1 );
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = 
                manager.createStore( new StoreIdImpl( storeName ) );
            
            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = 
                store.createReader( readerId1 );
            
            store.enqueue( new TestEntry("Entry1" ), 0, null );
            store.enqueue( new TestEntry("Entry2" ), 0, null );
            
            assertEquals( 1, store.getNumBuckets() );
            assertEquals( new AckIdV3( 0, false ),  store.getMinAvailableLevel() );
            assertEquals( 2, store.getStoreMetrics().getTotalMessageCount().getEntryCount() );
            assertEquals( 2, reader.getStoreBacklogMetrics().getQueueDepth() );
            assertEquals( new AckIdV3( 0, false ), reader.getAckLevel() );
            
            InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
            
            assertNotNull( entry );
            assertEquals( "Entry1", entry.getLogId() );
            reader.ack( entry.getAckId() );
            
            entry = reader.dequeue();
            assertNotNull( entry );
            assertEquals( "Entry2", entry.getLogId() );
            reader.ack( entry.getAckId() );
            
            clock.increaseTime( getMaxPeriodMS() + 1 );
            store.runCleanupNow();
            
            assertEquals( 0, store.getNumBuckets() );
            assertEquals( StoredCount.EmptyStoredCount, store.getStoreMetrics().getTotalMessageCount() );
            assertEquals( StoredCount.EmptyStoredCount, reader.getStoreBacklogMetrics().getTotalMessageCount() );
            
            clock.increaseTime( 1 );
            store.enqueue( new TestEntry("Entry3" ), 0, null );
            
            assertEquals( 1, store.getNumBuckets() );
            assertEquals( 1, store.getStoreMetrics().getTotalMessageCount().getEntryCount() );
            assertEquals( 1, reader.getStoreBacklogMetrics().getQueueDepth() );
            
            entry = reader.dequeue();
            assertNotNull( entry );
            assertEquals( "Entry3", entry.getLogId() );
            reader.ack( entry.getAckId() );
            
            store.runCleanupNow();
            assertEquals( 1, store.getNumBuckets() );
            assertEquals( 1, store.getStoreMetrics().getTotalMessageCount().getEntryCount() );
            assertEquals( 0, reader.getStoreBacklogMetrics().getQueueDepth() );
            
        } finally {
            if( manager != null ) manager.close();
        }
    }
    
    @Test
    public void testBucketCleanupWithOneReader()
        throws InvalidConfigException, SeqStoreException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final long bucketPeriod = getMaxPeriodMS();
        int redeliveryTimeout = (int) bucketPeriod * 100;

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, redeliveryTimeout);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = store.createReader(readerId1);

            // Bucket 1
            store.enqueue(new TestEntry("Entry1", initialTime), 0, null);
            store.enqueue(new TestEntry("Entry1", initialTime + 1), 0, null);

            // Bucket 2
            store.enqueue(new TestEntry("Entry2", initialTime + 4 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 4 * bucketPeriod + 1), 0, null);

            // Bucket 3
            store.enqueue(new TestEntry("Entry2", initialTime + 6 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 6 * bucketPeriod + 1), 0, null);

            // Bucket 4
            store.enqueue(new TestEntry("Entry2", initialTime + 7 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 7 * bucketPeriod + 1), 0, null);

            assertEquals(4, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(8, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(7, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(0, false), reader.getAckLevel());

            store.runCleanupNow();

            assertEquals(4, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(8, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());

            // Bump time forward
            clock.setCurrentTime(initialTime + 7 * bucketPeriod + 2);

            AckIdV3 skipped = null;
            // Dequeue and ack all messages except the second one in bucket 2
            int cnt = 0;
            InflightEntry<AckIdV3, BasicInflightInfo> entry;
            while ((entry = reader.dequeue()) != null) {
                if (cnt != 3)
                    reader.ack(entry.getAckId());
                else
                    skipped = entry.getAckId();
                cnt++;
            }

            assertNotNull(skipped);

            store.runCleanupNow();

            assertEquals(2, store.getNumBuckets());
            assertEquals(new AckIdV3(skipped, false), store.getMinAvailableLevel());
            assertEquals(4, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(skipped, false), reader.getAckLevel());

            clock.increaseTime(redeliveryTimeout);

            entry = reader.dequeue();
            assertNotNull(entry);
            assertEquals(skipped, entry.getAckId());
        } finally {
            if (manager != null)
                manager.close();
        }
    }

    @Test
    public void testBucketCleanupWithMultipleReaders()
        throws InvalidConfigException, SeqStoreException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final String readerId2 = "Reader2";
        final long bucketPeriod = getMaxPeriodMS();
        int redeliveryTimeout = (int) bucketPeriod * 100;

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, redeliveryTimeout);
        setupReaderConfig(configProvider, storeName, readerId2, redeliveryTimeout);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader1 = store.createReader(readerId1);

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader2 = store.createReader(readerId2);

            // Bucket 1
            store.enqueue(new TestEntry("Entry1", initialTime), 0, null);
            store.enqueue(new TestEntry("Entry1", initialTime + 1), 0, null);

            // Bucket 2
            store.enqueue(new TestEntry("Entry2", initialTime + 4 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 4 * bucketPeriod + 1), 0, null);

            // Bucket 3
            store.enqueue(new TestEntry("Entry2", initialTime + 6 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 6 * bucketPeriod + 1), 0, null);

            // Bucket 4
            store.enqueue(new TestEntry("Entry2", initialTime + 7 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 7 * bucketPeriod + 1), 0, null);

            assertEquals(4, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(8, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(7, reader1.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(0, false), reader1.getAckLevel());

            store.runCleanupNow();

            assertEquals(4, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(8, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(1, reader2.getStoreBacklogMetrics().getQueueDepth());

            // Bump time forward
            clock.setCurrentTime(initialTime + 7 * bucketPeriod + 2);

            AckIdV3 skipped1 = null;
            // Dequeue and ack all messages from reader1 except the second one
            // in bucket 2
            int cnt = 0;
            InflightEntry<AckIdV3, BasicInflightInfo> entry;
            while ((entry = reader1.dequeue()) != null) {
                if (cnt != 4)
                    reader1.ack(entry.getAckId());
                else
                    skipped1 = entry.getAckId();
                cnt++;
            }

            AckIdV3 skipped2 = null;
            // Dequeue and ack all messages from reader2 except the first one in
            // bucket 3
            cnt = 0;
            while ((entry = reader2.dequeue()) != null) {
                if (cnt != 3)
                    reader2.ack(entry.getAckId());
                else
                    skipped2 = entry.getAckId();
                cnt++;
            }

            assertNotNull(skipped2);

            AckIdV3 minSkipped = AckIdV3.min(skipped1, skipped2);

            store.runCleanupNow();

            assertEquals(3, store.getNumBuckets());
            assertEquals(new AckIdV3(minSkipped, false), store.getMinAvailableLevel());
            assertEquals(6, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader1.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(skipped1, false), reader1.getAckLevel());
            assertEquals(new AckIdV3(skipped2, false), reader2.getAckLevel());

            clock.increaseTime(redeliveryTimeout);

            entry = reader1.dequeue();
            assertNotNull(entry);
            assertEquals(skipped1, entry.getAckId());

            entry = reader2.dequeue();
            assertNotNull(entry);
            assertEquals(skipped2, entry.getAckId());
        } finally {
            if (manager != null)
                manager.close();
        }
    }

    @Test
    public void testBucketCleanupWithGauranteedRetention()
        throws InvalidConfigException, SeqStoreException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final String readerId2 = "Reader2";
        final long bucketPeriod = getMaxPeriodMS();
        int redeliveryTimeout = (int) bucketPeriod * 100;

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 2 * bucketPeriod, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, redeliveryTimeout);
        setupReaderConfig(configProvider, storeName, readerId2, redeliveryTimeout);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = store.createReader(readerId1);

            // Bucket 1
            store.enqueue(new TestEntry("Entry1", initialTime), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 1), 0, null);

            // Bucket 2
            store.enqueue(new TestEntry("Entry3", initialTime + 2 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry4", initialTime + 2 * bucketPeriod + 1), 0, null);

            // Bucket 3
            store.enqueue(new TestEntry("Entry5", initialTime + 4 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry6", initialTime + 4 * bucketPeriod + 1), 0, null);

            // Bucket 4
            store.enqueue(new TestEntry("Entry7", initialTime + 6 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry8", initialTime + 6 * bucketPeriod + 1), 0, null);

            // Bucket 5
            store.enqueue(new TestEntry("Entry9", initialTime + 7 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry10", initialTime + 7 * bucketPeriod + 1), 0, null);
            store.enqueue(new TestEntry("Entry11", initialTime + 7 * bucketPeriod + 2), 0, null);

            assertEquals(5, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(11, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(10, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(0, false), reader.getAckLevel());

            store.runCleanupNow();

            assertEquals(5, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(11, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());

            // Bump time forward
            clock.setCurrentTime(initialTime + 7 * bucketPeriod + 2);

            AckIdV3 skipped = null;
            // Dequeue and ack all messages except the second one in bucket 1
            int cnt = 0;
            InflightEntry<AckIdV3, BasicInflightInfo> entry;
            while ((entry = reader.dequeue()) != null) {
                if (cnt != 1)
                    reader.ack(entry.getAckId());
                else
                    skipped = entry.getAckId();
                cnt++;
            }

            assertNotNull(skipped);

            store.runCleanupNow();

            assertEquals(3, store.getNumBuckets());
            assertEquals(new AckIdV3(skipped, false), store.getMinAvailableLevel());
            assertEquals(7, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(skipped, false), reader.getAckLevel());

            clock.increaseTime(redeliveryTimeout);

            entry = reader.dequeue();
            assertNotNull(entry);
            assertEquals(skipped, entry.getAckId());

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader2 = store.createReader(readerId2);
            entry = reader2.dequeue();
            assertNotNull(entry);
            assertEquals("Entry2", entry.getLogId());
            entry = reader2.dequeue();
            assertNotNull(entry);
            assertEquals("Entry7", entry.getLogId());
        } finally {
            if (manager != null)
                manager.close();
        }
    }

    @Test
    public void testBucketCleanupMiddleBuckets()
        throws InvalidConfigException, SeqStoreException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final long bucketPeriod = getMaxPeriodMS();
        int redeliveryTimeout = (int) bucketPeriod * 100;

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, redeliveryTimeout);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = store.createReader(readerId1);

            // Bucket 1
            store.enqueue(new TestEntry("Entry1", initialTime), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 1), 0, null);

            // Bucket 2
            store.enqueue(new TestEntry("Entry3", initialTime + 4 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry4", initialTime + 4 * bucketPeriod + 1), 0, null);

            // Bucket 3
            store.enqueue(new TestEntry("Entry5", initialTime + 6 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry6", initialTime + 6 * bucketPeriod + 1), 0, null);

            // Bucket 4
            store.enqueue(new TestEntry("Entry7", initialTime + 7 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry8", initialTime + 7 * bucketPeriod + 1), 0, null);

            // Bucket 5
            store.enqueue(new TestEntry("Entry9", initialTime + 9 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry10", initialTime + 9 * bucketPeriod + 1), 0, null);

            assertEquals(5, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(10, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(9, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(0, false), reader.getAckLevel());

            store.runCleanupNow();

            assertEquals(5, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(10, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());

            // Bump time forward
            clock.setCurrentTime(initialTime + 9 * bucketPeriod + 2);

            AckIdV3 first = null;
            // buckets 3 & 4
            for (int cnt = 0; cnt < 8; ++cnt) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                if (first == null)
                    first = entry.getAckId();
                if (cnt >= 4) {
                    reader.ack(entry.getAckId());
                }
            }

            store.runCleanupNow();

            assertEquals(3, store.getNumBuckets());
            assertEquals(new AckIdV3(first, false), store.getMinAvailableLevel());
            assertEquals(6, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(2, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(first, false), reader.getAckLevel());

            clock.increaseTime(redeliveryTimeout);

            // Dequeue and ack everything left
            for (int cnt = 0; cnt < 4; cnt++) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                reader.ack(entry.getAckId());
            }

            for (int cnt = 8; cnt < 10; cnt++) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                reader.ack(entry.getAckId());
            }

            assertNull(reader.dequeue());
        } finally {
            if (manager != null)
                manager.close();
        }
    }

    @Test
    public void testBucketCleanupEndBuckets()
        throws InvalidConfigException, SeqStoreException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final long bucketPeriod = getMaxPeriodMS();
        int redeliveryTimeout = (int) bucketPeriod * 100;

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, redeliveryTimeout);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = store.createReader(readerId1);

            // Bucket 1
            store.enqueue(new TestEntry("Entry1", initialTime), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 1), 0, null);

            // Bucket 2
            store.enqueue(new TestEntry("Entry3", initialTime + 4 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry4", initialTime + 4 * bucketPeriod + 1), 0, null);

            // Bucket 3
            store.enqueue(new TestEntry("Entry5", initialTime + 6 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry6", initialTime + 6 * bucketPeriod + 1), 0, null);

            // Bucket 4
            store.enqueue(new TestEntry("Entry7", initialTime + 7 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry8", initialTime + 7 * bucketPeriod + 1), 0, null);

            // Bucket 5
            store.enqueue(new TestEntry("Entry9", initialTime + 9 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry10", initialTime + 9 * bucketPeriod + 1), 0, null);

            assertEquals(5, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(10, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(9, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(0, false), reader.getAckLevel());

            store.runCleanupNow();

            assertEquals(5, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(10, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader.getStoreBacklogMetrics().getQueueDepth());

            // Bump time forward
            clock.setCurrentTime(initialTime + 9 * bucketPeriod + 2);

            AckIdV3 first = null;
            // Dequeue all messages buckets and ack those in buckets 3, 4 and 5
            for (int cnt = 0; cnt < 10; ++cnt) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                if (first == null)
                    first = entry.getAckId();
                if (cnt >= 4)
                    reader.ack(entry.getAckId());
            }

            store.runCleanupNow();

            // Bucket 5 can't be deleted because it might get more messages
            assertEquals(3, store.getNumBuckets());
            assertEquals(new AckIdV3(first, false), store.getMinAvailableLevel());
            assertEquals(6, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals(new AckIdV3(first, false), reader.getAckLevel());

            clock.increaseTime(redeliveryTimeout);

            // Dequeue and ack everything left
            for (int cnt = 0; cnt < 4; cnt++) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                reader.ack(entry.getAckId());
            }

            assertNull(reader.dequeue());
        } finally {
            if (manager != null)
                manager.close();
        }
    }
    
    @Test
    public void testBucketCleanupMultipleReaders()
        throws InvalidConfigException, SeqStoreException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final String readerId2 = "Reader2";
        final long bucketPeriod = getMaxPeriodMS();
        int redeliveryTimeout = (int) bucketPeriod * 100;

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);

        setupStoreConfig(configProvider, storeName, 1, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, redeliveryTimeout);
        setupReaderConfig(configProvider, storeName, readerId2, redeliveryTimeout);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl(
                    storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader1 = store.createReader(readerId1);
            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader2 = store.createReader(readerId2);

            // Bucket 1
            store.enqueue(new TestEntry("Entry1", initialTime), 0, null);
            store.enqueue(new TestEntry("Entry2", initialTime + 1), 0, null);

            // Bucket 2
            store.enqueue(new TestEntry("Entry3", initialTime + 4 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry4", initialTime + 4 * bucketPeriod + 1), 0, null);

            // Bucket 3
            store.enqueue(new TestEntry("Entry5", initialTime + 6 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry6", initialTime + 6 * bucketPeriod + 1), 0, null);

            // Bucket 4
            store.enqueue(new TestEntry("Entry7", initialTime + 7 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry8", initialTime + 7 * bucketPeriod + 1), 0, null);

            // Bucket 5
            store.enqueue(new TestEntry("Entry9", initialTime + 9 * bucketPeriod), 0, null);
            store.enqueue(new TestEntry("Entry10", initialTime + 9 * bucketPeriod + 1), 0, null);

            assertEquals(5, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(10, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(9, reader1.getStoreBacklogMetrics().getDelayedMessageCount().getEntryCount());
            assertEquals( reader2.getStoreBacklogMetrics(), reader1.getStoreBacklogMetrics() );
            assertEquals( AckIdV3.MINIMUM, reader1.getAckLevel());
            assertEquals( reader2.getAckLevel(), reader1.getAckLevel() );

            store.runCleanupNow();

            assertEquals(5, store.getNumBuckets());
            assertEquals(new AckIdV3(0, false), store.getMinAvailableLevel());
            assertEquals(10, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(1, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals( reader2.getStoreBacklogMetrics(), reader1.getStoreBacklogMetrics() );

            // Bump time forward
            clock.setCurrentTime(initialTime + 12 * bucketPeriod + 2);

            AckIdV3 first = null;
            // Ack everything in buckets 3 and 4 for reader1
            for (int cnt = 0; cnt < 8; ++cnt) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader1.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                if (first == null)
                    first = entry.getAckId();
                if (cnt >= 4) {
                    reader1.ack(entry.getAckId());
                }
            }

            // Shouldn't be able to do anything as reader2 hasn't acked anything
            store.runCleanupNow();

            assertEquals(5, store.getNumBuckets());
            assertEquals(2, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(10, reader2.getStoreBacklogMetrics().getQueueDepth());
            
            // Ack everything in buckets 1, 4 and 5 for reader2
            AckIdV3 r2FirstNotAckedId = null;
            for (int cnt = 0; cnt < 10; ++cnt) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader2.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                if (cnt < 2 || cnt >= 6 ) {
                    reader2.ack( entry.getAckId() );
                } else if( r2FirstNotAckedId == null ) {
                    r2FirstNotAckedId = entry.getAckId();
                }
            }
            assertNull( reader2.dequeue() );
            
            // Bucket 4 should be deleted
            store.runCleanupNow();
            
            assertEquals(4, store.getNumBuckets());
            assertEquals(new AckIdV3(first, false), store.getMinAvailableLevel());
            assertEquals(8, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(2, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader2.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(new AckIdV3(first, false), reader1.getAckLevel());
            assertEquals(new AckIdV3(r2FirstNotAckedId, false), reader2.getAckLevel());
            
            clock.increaseTime(redeliveryTimeout);

            // Dequeue and ack everything left for reader1
            for (int cnt = 0; cnt < 4; cnt++) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader1.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                reader1.ack(entry.getAckId());
            }

            for (int cnt = 8; cnt < 10; cnt++) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader1.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 1), entry.getLogId());
                reader1.ack(entry.getAckId());
            }

            assertNull(reader1.dequeue());
            
            // Only buckets 2, 3 and 5 should be left - 5 is kept because it contains last available
            store.runCleanupNow();
            
            assertEquals(3, store.getNumBuckets());
            assertEquals(new AckIdV3(r2FirstNotAckedId, false), store.getMinAvailableLevel());
            assertEquals(6, store.getStoreMetrics().getTotalMessageCount().getEntryCount());
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader2.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(new AckIdV3(r2FirstNotAckedId, false), reader2.getAckLevel());
            
            // Dequeue and ack everything left for reader2
            for (int cnt = 0; cnt < 4; cnt++) {
                InflightEntry<AckIdV3, BasicInflightInfo> entry = reader2.dequeue();
                assertNotNull(entry);
                assertEquals("Entry" + (cnt + 3), entry.getLogId());
                reader2.ack(entry.getAckId());
            }
            
            assertNull(reader2.dequeue());
            
            // Everything should be cleaned
            store.runCleanupNow();
            
            assertEquals(0, store.getNumBuckets());
            assertTrue( store.isEmpty() );
            assertEquals(0, reader1.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0, reader2.getStoreBacklogMetrics().getQueueDepth());
        } finally {
            if (manager != null)
                manager.close();
        }
    }
    
    @Test
    public void testEvictFromCacheAfterDequeueWithOneReader()
        throws SeqStoreException, InvalidConfigException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final long bucketPeriod = getMaxPeriodMS();

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);
        long cacheTime = 
                TimeUnit.SECONDS.toMillis(
                        STORE_CONFIG.getMaxEstimatedCacheTimeSeconds() );

        setupStoreConfig(configProvider, storeName, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, 1);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = 
                    manager.createStore(new StoreIdImpl(storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader = store.createReader(readerId1);

            store.enqueue(new TestEntry("Entry1", 1), 0, null);
            store.enqueue(new TestEntry("Entry2", 1), 0, null);
            
            store.runCleanupNow();
            
            // The reader is right there
            assertTrue( store.isCacheEnqueues() );
            assertTrue( reader.isEvictFromCacheAfterDequeueFromStore() );
            
            clock.increaseTime(cacheTime+1);
            store.runCleanupNow();
            // The readers is too far behind
            assertFalse( store.isCacheEnqueues() );
            assertTrue( reader.isEvictFromCacheAfterDequeueFromStore() );
            
            store.enqueue(new TestEntry("Entry3", 1), 0, null);
            store.enqueue(new TestEntry("Entry4", 1), 0, null);
            
            // Move reader forward but not far enough to catch up
            reader.dequeueFromStore();
            
            store.runCleanupNow();
            
            assertFalse( store.isCacheEnqueues() );
            assertTrue( reader.isEvictFromCacheAfterDequeueFromStore() );
            
            // Move reader 2 forward twice - now it should be close enough to start caching again
            reader.dequeueFromStore();
            reader.dequeueFromStore();
            
            assertFalse( store.isCacheEnqueues() );
            assertTrue( reader.isEvictFromCacheAfterDequeueFromStore() );
        } finally {
            if (manager != null)
                manager.close();
        }
    }
    
    @Test
    public void testEvictFromCacheAfterDequeueWithMultipleReaders()
        throws SeqStoreException, InvalidConfigException, IOException, InterruptedException
    {
        final String storeName = "Store";
        final String readerId1 = "Reader1";
        final String readerId2 = "Reader2";
        final long bucketPeriod = getMaxPeriodMS();

        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();

        SettableClock clock = new SettableClock();
        long initialTime = bucketPeriod * 2;
        clock.setCurrentTime(initialTime);
        long cacheTime = 
                TimeUnit.SECONDS.toMillis(
                        STORE_CONFIG.getMaxEstimatedCacheTimeSeconds() );

        setupStoreConfig(configProvider, storeName, 0, Long.MAX_VALUE);
        setupReaderConfig(configProvider, storeName, readerId1, 1);
        setupReaderConfig(configProvider, storeName, readerId2, 1);

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, true);
        try {
            SeqStoreInternalInterface<BasicInflightInfo> store = 
                    manager.createStore(new StoreIdImpl(storeName));

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader1 = store.createReader(readerId1);

            SeqStoreReaderV3InternalInterface<BasicInflightInfo> reader2 = store.createReader(readerId2);

            store.enqueue(new TestEntry("Entry1", 1), 0, null);
            store.enqueue(new TestEntry("Entry2", 1), 0, null);
            
            store.runCleanupNow();
            
            // The readers are right there
            assertTrue( store.isCacheEnqueues() );
            assertFalse( reader1.isEvictFromCacheAfterDequeueFromStore() );
            assertFalse( reader2.isEvictFromCacheAfterDequeueFromStore() );
            
            clock.increaseTime(cacheTime+1);
            store.runCleanupNow();
            // The readers are too far behind
            assertFalse( store.isCacheEnqueues() );
            // But they're at the same place and shouldn't evict anything from the cache
            // after reading - it will be deleted soon enough anyway
            assertFalse( reader1.isEvictFromCacheAfterDequeueFromStore() );
            assertFalse( reader2.isEvictFromCacheAfterDequeueFromStore() );
            
            store.enqueue(new TestEntry("Entry3", 1), 0, null);
            store.enqueue(new TestEntry("Entry4", 1), 0, null);
            
            // Move reader 2 forward - that shouldn't chance caching as they are still
            // right next to eachother
            reader2.dequeueFromStore();
            
            store.runCleanupNow();
            
            assertFalse( reader1.isEvictFromCacheAfterDequeueFromStore() );
            assertFalse( reader2.isEvictFromCacheAfterDequeueFromStore() );
            
            // Move reader 2 forward twice - now the gap should be large enough
            reader2.dequeueFromStore();
            reader2.dequeueFromStore();
            store.runCleanupNow();
            
            assertTrue( reader1.isEvictFromCacheAfterDequeueFromStore() );
            assertTrue( reader2.isEvictFromCacheAfterDequeueFromStore() );
            
            // reader2 is close enough that messages should be cached
            assertTrue( store.isCacheEnqueues() );
            
            // Have reader 1 catch up
            reader1.dequeueFromStore();
            reader1.dequeueFromStore();
            reader1.dequeueFromStore();
            reader1.dequeueFromStore();
            
            store.runCleanupNow();
            
            assertFalse( reader1.isEvictFromCacheAfterDequeueFromStore() );
            assertFalse( reader2.isEvictFromCacheAfterDequeueFromStore() );
            assertTrue( store.isCacheEnqueues() );
        } finally {
            if (manager != null)
                manager.close();
        }
    }
}
