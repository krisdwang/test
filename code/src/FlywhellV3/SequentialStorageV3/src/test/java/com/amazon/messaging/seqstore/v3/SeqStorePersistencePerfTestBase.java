package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Before;
import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.mapPersistence.SeqStorePersistencePerfTest;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

@SuppressWarnings("FL_MATH_USING_FLOAT_PRECISION")
public abstract class SeqStorePersistencePerfTestBase {
    private static final class RealThroughputEnqueuer extends Thread {

        private final String payload;

        private final SeqStoreInternalInterface<BasicInflightInfo> store;

        private final AtomicBoolean keepConsuming;

        private RealThroughputEnqueuer(String name, String payload,
                SeqStoreInternalInterface<BasicInflightInfo> store, AtomicBoolean keepConsuming) {
            super(name);
            this.payload = payload;
            this.store = store;
            this.keepConsuming = keepConsuming;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < count; i++) {
                    store.enqueue(new TestEntry(payload), -1, null);
                }
                keepConsuming.set(false);
            } catch (Throwable e) {
                log.error("failure", e);
            }
        }
    }

    private final class EnqueueThread extends Thread {

        private final SeqStore<AckIdV3, BasicInflightInfo> store;

        private final String payload;

        private EnqueueThread(String name, SeqStore<AckIdV3, BasicInflightInfo> store, String payload) {
            super(name);
            this.store = store;
            this.payload = payload;
        }

        @Override
        public void run() {
            int enqueued = 0;
            try {
                for (int i = 0; i < count + 1; i++) {
                    store.enqueue(new TestEntry(payload), -1, null);
                    enqueued++;
                }
                System.out.println("Done enqueueing. enqueued " + enqueued + " messages");
            } catch (Throwable e) {
                log.error("failure", e);
                error = e;
            }
        }
    }

    static final Log log = LogFactory.getLog(SeqStorePersistencePerfTest.class);

    /**
     * Number of messages to enqueue/dequeue
     */
    private static final int count = 10000;

    /**
     * Number of concurrent readers
     */
    private final static int readerCount = 2;

    protected static final StoreId STOREID1 = new StoreIdImpl("SewStorePersistencePerfStore", "1");

    protected static final StoreId STOREID2 = new StoreIdImpl("SewStorePersistencePerfStore", "2");

    /**
     * Use thread per reader or single thread that iterates across all readers
     */
    private static boolean useOneReaderThread = false;

    private final int payloadSize;

    private boolean isNullTest = false;

    private boolean isPreloading = false;

    private static int testNumber = 0;
    
    private final boolean isCoveragePass;

    Throwable error;

    protected BasicConfigProvider<BasicInflightInfo> confProvider =
    	BasicConfigProvider.newBasicInflightConfigProvider();

    protected SeqStorePersistencePerfTestBase(int payloadSize) {
        String isCoveragePassStr = 
                System.getProperties().getProperty( "dse.RunAsCoverageTest" );
        if( isCoveragePassStr != null && (
                isCoveragePassStr.equalsIgnoreCase( "true") || 
                isCoveragePassStr.equalsIgnoreCase( "t" ) ) ) 
        {
            isCoveragePass = true;
        } else {
            isCoveragePass = false;
        }
        
        this.payloadSize = payloadSize;
        Logger.getRootLogger().setLevel(Level.INFO);
        Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout("%d\t%-5p [%t]: %m%n")));
    }

    protected abstract SeqStoreManagerV3<BasicInflightInfo> setupManager(Clock clock)
            throws SeqStoreException, IOException;

    public abstract void shutdownManager(SeqStoreManagerV3<BasicInflightInfo> manager) throws SeqStoreException;

    /**
     * Enqueue all messages then dequeue all of them using concurrent readers.
     */
    @Test
    public void testEnqueueThenDequeue() throws Throwable {
        if( isCoveragePass ) return;
        
        System.out.println("testEnqueueThenDequeue begins");
        isNullTest = false;
        final SeqStoreManagerV3<BasicInflightInfo> manager = setupManager(new AlwaysIncreasingClock());

        SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(STOREID2);

        checkStoresExist(manager);

        for (int i = 0; i < readerCount; i++) {
            String readerId = "reader-" + i;
            registerReader(store, readerId);
        }
        StringBuffer payloadB = new StringBuffer();
        for (int i = 0; i < payloadSize; i++) {
            payloadB.append('a');
        }
        String payload = payloadB.toString();
        // AckIdV3 lastEnqueuedAckId = store.getLastEnqueuedAckId();
        /*
         * int last; if (lastEnqueuedAckId != BinaryData.ZERO) { String s = new
         * String(lastEnqueuedAckId.getBytes(), "UTF-8"); last =
         * Integer.parseInt(s); } else { last = 0; } StringBuffer payloadB = new
         * StringBuffer(); for (int i = 0; i < payloadSize; i++) {
         * payloadB.append('a'); } String payload = payloadB.toString(); for
         * (int i = 0; i < readerCount; i++) { registerReader(store, "reader-" +
         * i); } long time = System.currentTimeMillis(); for (int i = last + 1;
         * i < last + count + 1; i++) { store.enqueue(new
         * StoredEntryImpl(ackIdGen_.next(), "metadata" + i, payload)); }
         */
        // preload the class
        long time = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            store.enqueue(new TestEntry(payload), -1, null);
        }
        time = System.currentTimeMillis() - time;
        System.out.println("testEnqueueThenDequeue: Enqueue Mesages/second="
                + (((float) count) * 1000 / time) + ". Message size is " + payloadSize + " bytes.");
        // V3 has to be faster than this for sure
        double though = ((float) count) * 1000 / time;
        assertTrue(though > 500);
        if (useOneReaderThread) {
            dequeueMessagesOneThread(store);
        } else {
            dequeueMessagesManyThreads(store);
        }
        shutdownManager(manager);
        log.info("testEnqueueThenDequeue end");
    }

    private void checkStoresExist(final SeqStoreManager<AckIdV3, BasicInflightInfo> manager)
        throws SeqStoreDatabaseException, SeqStoreClosedException 
    {
        assertTrue(manager.containsStore(STOREID1));
        assertTrue(manager.containsStore(STOREID2));
        assertEquals(2, manager.getStoreNames().size() );
    }

    private void preloadSingleThreadedEnqueue() throws Throwable {
        final SeqStoreManagerV3<BasicInflightInfo> manager = setupManager(new AlwaysIncreasingClock());
        final SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(STOREID2);

        System.out.println("Preloading classes");

        checkStoresExist(manager);

        for (int i = 0; i < readerCount; i++) {
            String readerId = "reader-" + i;
            registerReader(store, readerId);
        }
        StringBuffer payloadB = new StringBuffer();
        for (int i = 0; i < payloadSize; i++) {
            payloadB.append('a');
        }
        String payload = payloadB.toString();
        // preload the class
        store.enqueue(new TestEntry(payload), -1, null);
        for (int i = 0; i < count; i++) {
            store.enqueue(new TestEntry(payload), -1, null);
        }

        if (useOneReaderThread) {
            dequeueMessagesOneThread(store);
        } else {
            dequeueMessagesManyThreads(store);
        }

        shutdownManager(manager);
    }

    private void preloadMultithreadedEnqueue() throws Throwable {
        final SeqStoreManagerV3<BasicInflightInfo> manager = setupManager(new AlwaysIncreasingClock());
        try {
            final SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(STOREID2);
    
            StringBuffer payloadB = new StringBuffer();
            for (int i = 0; i < payloadSize; i++) {
                payloadB.append('a');
            }
            final String payload1 = payloadB.toString();
            Thread thread = new EnqueueThread("preloadMultithreadedEnqueueThread", store, payload1);
    
            SeqStoreReaderConfig<BasicInflightInfo> config = BasicConfigProvider.newBasicInflightReaderConfig();
            confProvider.putReaderConfig(store.getConfigKey(), "reader2", config);
            store.createReader("reader2");
    
            // SeqStoreReaderPersistence reader = store1.getReader("reader1");
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader2 = store.getReader("reader2");
            long time = System.currentTimeMillis();
            thread.start();
            
            // Dequeue and ack the first message
            StoredEntry<AckIdV3> dequeue = dequeueWithRetries( reader2, 200 );
            assertNotNull( dequeue );
            reader2.ack( dequeue.getAckId() );
            
            int dequeued = 0;
            int queueSize = 0;
            while (dequeued < count) {
                dequeue = dequeueWithRetries(reader2, 100);
                assertNotNull( "Could not read all messages. Failed after " + dequeued + " messages", dequeue );
                dequeued++;
                queueSize++;
            }
            time = System.currentTimeMillis() - time;
            thread.join();
            assertEquals(count, dequeued);
            if (error != null)
                throw error;
            // v3 has to be at least this speed
            double though = ((float) dequeued) * 1000 / time;
            assertTrue(though > 1000);
            store.removeReader("reader2");
        } finally {
            shutdownManager(manager);
        }
    }

    public StoredEntry<AckIdV3> dequeueWithRetries(SeqStoreReader<AckIdV3, BasicInflightInfo> reader, int attempts)
        throws SeqStoreException, InterruptedException
    {
        StoredEntry<AckIdV3> dequeue;
        dequeue = null;
        for( int i = 0; dequeue == null && i < attempts; ++i ) {
            dequeue = reader.dequeue();
            if( dequeue == null ) {
                Thread.sleep(1);
            }
        }
        return dequeue;
    }

    // for performance testing, we need to load all classes first
    @Before
    public void preLoad() throws Throwable {
        if( isCoveragePass ) return;
        
        Thread.currentThread().setName("main-" + (++testNumber));

        isPreloading = true;
        isNullTest = false;

        // Not sure why both are needed. Keeping them to avoid changing the unit
        // test compeltely.
        preloadSingleThreadedEnqueue();
        preloadMultithreadedEnqueue();

        System.out.println("Preloaded classes");
        isPreloading = false;
    }

    /**
     * Dequeue messages using thread per reader
     * 
     * @throws Throwable
     */
    private void dequeueMessagesManyThreads(final SeqStore<AckIdV3, BasicInflightInfo> store) throws Throwable {
        Thread[] readerThreads = new Thread[readerCount];
        for (int i = 0; i < readerCount; i++) {
            final int ii = i;
            readerThreads[i] = new Thread("dequeueMessagesManyThreads:" + i) {

                @Override
                public void run() {
                    try {
                        dequeueMessages(store, ii);
                    } catch (Throwable e) {
                        error = e;
                        log.error("failure dequeueing", e);
                    }
                }
            };
        }
        for (int i = 0; i < readerCount; i++) {
            readerThreads[i].start();
        }
        for (int i = 0; i < readerCount; i++) {
            readerThreads[i].join();
        }
        if (error != null) {
            throw error;
        }
    }

    /**
     * Dequeue messages in a calling thread
     * 
     * @see #dequeueMessagesManyThreads(int)
     */
    void dequeueMessages(SeqStore<AckIdV3, BasicInflightInfo> store, int i)
            throws SeqStoreException, InterruptedException {
        String readerId = "reader-" + i;
        long time;
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader2 = store.getReader(readerId);
        StoredEntry<AckIdV3> entry;
        int dequeued = 0;
        int queueSize = 0;
        time = System.currentTimeMillis();
        while (dequeued < count) {
            if (!isNullTest) {
                entry = dequeueWithRetries(reader2, 100);
                assertNotNull( entry );
                dequeued++;
                // System.out.print((new String(entry.getAckId().getBytes(),
                // "UTF-8")) + " ");
                queueSize++;
            } else {
                reader2.dequeue();
                dequeued++;
            }
        }
        // reader2.setAckLevel(lastAckId);
        time = System.currentTimeMillis() - time;
        // System.out.println("dequeueMessages: " + readerId + " - Dequeue
        // Mesages/second="
        // + (((float) dequeued) * 1000 / time));
        if (!isPreloading) {
            System.out.println("dequeueMessages: " + readerId + " consumed " + dequeued + " messages in "
                    + time + "ms.  Throughput Mesages/second=" + (((float) dequeued) * 1000 / time)
                    + ". Message size is " + payloadSize + " bytes.");
            double though = ((float) dequeued) * 1000 / time;
            assertTrue(though > 500);
        }
        store.removeReader(readerId);
    }

    /**
     * Dequeue messages using multiple readers and one thread
     */
    protected void dequeueMessagesOneThread(SeqStore<AckIdV3, BasicInflightInfo> store)
            throws SeqStoreException, InterruptedException {
        ArrayList<SeqStoreReader<AckIdV3, BasicInflightInfo>> readers = new ArrayList<SeqStoreReader<AckIdV3, BasicInflightInfo>>();
        for (int i = 0; i < readerCount; i++) {
            String readerId = "reader-" + i;
            readers.add(store.getReader(readerId));
        }
        StoredEntry<AckIdV3> entry;
        int[] queueSize = new int[readerCount];
        int[] dequeued = new int[readerCount];
        long time = System.currentTimeMillis();
        for (int i = 0; i < readerCount; i++) {
            while (dequeued[i] < count) {
                entry = dequeueWithRetries(readers.get(i), 100);
                if (entry != null) {
                    dequeued[i]++;
                    queueSize[i]++;
                }
            }
        }
        time = System.currentTimeMillis() - time;
        if (!isPreloading) {
            System.out.println("testEnqueueThenDequeue: " + "Readers:" + readerCount
                    + " - Dequeue Mesages/second=" + (((float) count) * 1000 / time));
            for (int i = 0; i < readerCount; i++) {
                assertEquals(count, dequeued[i]);
                store.removeReader("reader-" + i);
            }
        }

    }

    protected void registerReader(SeqStoreInternalInterface<BasicInflightInfo> store, String readerId) throws SeqStoreException {
        SeqStoreReaderConfig<BasicInflightInfo> config = BasicConfigProvider.newBasicInflightReaderConfig();
        confProvider.putReaderConfig(store.getConfigKey(), readerId, config);
        store.createReader(readerId);
    }

    /**
     * Test performance when there is no backlog. I.e. consumer is active. As
     * consumer blocks when there are no messages available performance can be
     * affected by notification speed between enqueueing and dequeing threads.
     * @throws Throwable 
     */
    @Test
    public void testThrough() throws Throwable {
        if( isCoveragePass ) return;
        
        final SeqStoreManagerV3<BasicInflightInfo> manager = setupManager(new AlwaysIncreasingClock());
        try {
            final SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(STOREID2);
    
            StringBuffer payloadB = new StringBuffer();
            for (int i = 0; i < payloadSize; i++) {
                payloadB.append('a');
            }
            final String payload = payloadB.toString();
            Thread thread = new EnqueueThread("testThroughEnqueueThread", store, payload);
    
            SeqStoreReaderConfig<BasicInflightInfo> config = BasicConfigProvider.newBasicInflightReaderConfig();
            confProvider.putReaderConfig(store.getConfigKey(), "reader2", config);
            store.createReader("reader2");
    
            // SeqStoreReaderPersistence reader = store1.getReader("reader1");
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader2 = store.getReader("reader2");
            long time = System.currentTimeMillis();
            thread.start();
            dequeueWithRetries(reader2, 100);
            StoredEntry<AckIdV3> dequeue;
            int dequeued = 0;
            int queueSize = 0;
            System.out.println("started testThrough");
            while (dequeued < count) {
                dequeue = dequeueWithRetries(reader2, 100);
                if (dequeue != null) {
                    dequeued++;
                    queueSize++;
                }
            }
            time = System.currentTimeMillis() - time;
            System.out.println("testThrough: Throughput Mesages/second=" + (((float) dequeued) * 1000 / time)
                    + ". Message size is " + payloadSize + " bytes.  Test took " + time);
            thread.join();
            assertEquals(count, dequeued);
            if (error != null)
                throw error;
            // v3 has to be at least this speed
            double through = ((float) dequeued) * 1000 / time;
            assertTrue("Throughput was only " + through, through > 1000);
            store.removeReader("reader2");
        } finally {
            shutdownManager(manager);
        }
    }

    /**
     * Test performance when there is no backlog. I.e. consumer is active. As
     * consumer blocks when there are no messages available performance can be
     * affected by notification speed between enqueueing and dequeing threads.
     * 
     * @throws Throwable
     */
    @Test
    public void testRealThroughputNoBacklog() throws Throwable {
        if( isCoveragePass ) return;
        
        final AtomicBoolean keepConsuming = new AtomicBoolean(true);
        final SeqStoreManagerV3<BasicInflightInfo> manager = setupManager(new AlwaysIncreasingClock());
        try {
            final SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(STOREID2);
    
            StringBuffer payloadB = new StringBuffer();
            for (int i = 0; i < payloadSize; i++) {
                payloadB.append('a');
            }
            final String payload = payloadB.toString();
    
            Thread thread = new RealThroughputEnqueuer("testRealThroughputNoBacklog", payload, store, keepConsuming);
    
            SeqStoreReaderConfig<BasicInflightInfo> config = BasicConfigProvider.newBasicInflightReaderConfig();
            confProvider.putReaderConfig(store.getConfigKey(), "reader2", config);
            store.createReader("reader2");
    
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader2 = store.getReader("reader2");
            dequeueWithRetries(reader2, 100);
            int queueSize = 0;
            if (!useOneReaderThread) {
                for (int i = 0; i < readerCount; i++) {
                    registerReader(store, "reader-" + i);
                }
            }
    
            long time = System.currentTimeMillis();
            System.out.println("started testRealThroughputNoBacklog");
            if (useOneReaderThread) {
                thread.start();
                int dequeued = 0;
                long lastDequeueTime = time;
                while (keepConsuming.get() && (dequeueWithRetries(reader2, 100) != null)) {
                    lastDequeueTime = System.currentTimeMillis();
                    dequeued++;
                    queueSize++;
                    /*
                     * if (queueSize > inFlightCount) {
                     * reader2.setAckLevel((BinaryData) queue.dequeue());
                     * queueSize--; }
                     */
                }
                // reader2.setAckLevel(lastAckId);
                time = lastDequeueTime - time;
                System.out.println("testRealThroughputNoBacklog: Consumed " + dequeued + " messages in " + time
                        + "ms.  Throughput Mesages/second=" + (((float) dequeued) * 1000 / time)
                        + ". Message size is " + payloadSize + " bytes.");
                double though = ((float) dequeued) * 1000 / time;
                assertTrue(though > 1000);
    
            } else {
                thread.start();
                dequeueMessagesManyThreads(store);
            }
            thread.join();
    
            if (!useOneReaderThread) {
                time = System.currentTimeMillis() - time;
                System.out.println("testRealThroughputNoBacklog: Finished in " + time + "ms. Message size is "
                        + payloadSize + " bytes.");
            }
    
            store.removeReader("reader2");
        } finally {
            shutdownManager(manager);
        }
        System.out.println("testRealThroughputNoBacklog test end.");
    }

    @Test
    public void testDequeueWithNothingIn() throws Throwable {
        if( isCoveragePass ) return;
        
        isNullTest = true;
        final SeqStoreManagerV3<BasicInflightInfo> manager = setupManager(new AlwaysIncreasingClock());

        try {
            final SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(STOREID2);
            checkStoresExist(manager);

            for (int i = 0; i < readerCount; i++) {
                String readerId = "reader-" + i;
                registerReader(store, readerId);
            }

            dequeueMessagesManyThreads(store);
        } finally {
            shutdownManager(manager);
        }

    }

    @Test
    public void testCleanup() throws Exception {
        if( isCoveragePass ) return;
        
        SettableClock clock = new SettableClock();

        final SeqStoreManagerV3<BasicInflightInfo> manager = setupManager(clock);
        try {
            checkStoresExist(manager);
    
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(STOREID1);
            SeqStoreInternalInterface<BasicInflightInfo> store2 = manager.getStore(STOREID2);
    
            setMaxRetentionPeriod(store, 1);
            setMaxRetentionPeriod(store2, 1);
    
            System.out.println("cleanup begins");
    
            long start = System.currentTimeMillis();
            clock.setCurrentTime(start);
    
            // long t = time;
            StringBuffer payloadB = new StringBuffer(payloadSize);
            for (int i = 0; i < payloadSize; i++) {
                payloadB.append('a');
            }
            String payload = payloadB.toString();
            List<SeqStoreReader<AckIdV3, BasicInflightInfo>> readers =
            	new ArrayList<SeqStoreReader<AckIdV3,BasicInflightInfo>>();
            List<SeqStoreReader<AckIdV3, BasicInflightInfo>> readers2 =
            	new ArrayList<SeqStoreReader<AckIdV3,BasicInflightInfo>>();
            for (int i = 0; i < readerCount; i++) {
                registerReader(store, "reader-" + i);
                readers.add(store.getReader("reader-" + i));
                registerReader(store2, "reader-" + i);
                readers2.add(store2.getReader("reader-" + i));
            }
    
            for (int i = 1; i < count + 1; i++) {
                store.enqueue(new TestEntry(payload.getBytes()), -1, null);
                store2.enqueue(new TestEntry(payload.getBytes()), -1, null);
            }
    
            long end = System.currentTimeMillis();
            System.out.println("cleanup: Enqueue Mesages/second=" + (((float) count * 2) * 1000 / (end - start))
                    + ". Message size is " + payloadSize + " bytes.");
    
            // Two seconds in the future so all messages will have expired.
            clock.setCurrentTime(start + 2000);
    
            store.runCleanupNow();
            store2.runCleanupNow();
    
            System.out.println("cleanup mesages/second="
                    + (((float) count * 2) * 1000 / (System.currentTimeMillis() - end)));
            registerReader(store, "reader--");
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.getReader("reader--");
            assertNull(reader.dequeue());
        } finally {
            shutdownManager(manager);
        }
    }

    private void setMaxRetentionPeriod(SeqStoreInternalInterface<BasicInflightInfo> store, int seconds) 
        throws SeqStoreException 
    {
        // All the stores use the default config except for the meax message lifetime
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setMaxMessageLifeTimeSeconds(seconds);
        confProvider.putStoreConfig(store.getConfigKey(), storeConfig);
        try {
            store.updateConfig();
        } catch (SeqStoreMissingConfigException e) {
            fail( "Store could not find the config that was just set.");
        }
    }

}
