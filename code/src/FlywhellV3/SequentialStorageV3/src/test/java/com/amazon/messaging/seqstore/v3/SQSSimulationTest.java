package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.bdb.BDBStoreManager;
import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

//SeqStoreManagerPersistenceConfig needs to be changed for larger cache size for simulating large number of stores
public class SQSSimulationTest {

    private boolean error_ = false;

    byte[] payload;

    protected static final AckIdGenerator ackIdGen_ = new AckIdGenerator();

    private static final int MessagesPerStore_ = 1000;

    private static final int payloadSize = 256;

    BDBStoreManager<BasicInflightInfo> manager_;

    private final ConcurrentHashMap<StoreType, Integer> largestStoreIds_ = new ConcurrentHashMap<StoreType, Integer>();

    private volatile boolean shouldStop = false;

    final ConcurrentHashMap<StoreId, Integer> numReaderMap_ = new ConcurrentHashMap<StoreId, Integer>();

    private static final int numReaderPerStore = 1;

    private static final int numThread_ = 2;

    private ArrayList<Executor> excutors_;

    private final ConcurrentHashMap<Long, Double> performanceHistory = new ConcurrentHashMap<Long, Double>();

    private final ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<Throwable>();

    private enum StoreType {
        PRIMARY,
        SECONDARY,
        TERTIARY,
        EMPTY_DEQUEUE,
        EMPTY
    }

    // accumulated percentage
    private static final int percentagePrimaryEnqueue = 10;

    private static final int percentagePrimaryDequeue = 10 + percentagePrimaryEnqueue;

    private static final int percentagePrimaryAck = 10 + percentagePrimaryDequeue;

    private static final int percentageSecondaryEnqueue = 10 + percentagePrimaryAck;

    private static final int percentageTertiaryEnqueue = 10 + percentageSecondaryEnqueue;

    private static final int percentageEmptyDequeue = 90 + percentageTertiaryEnqueue;

    private static final int total_ = percentageEmptyDequeue;

    private final BasicConfigProvider<BasicInflightInfo> confProvider =
    	BasicConfigProvider.newBasicInflightConfigProvider();

    static class TestingEntry extends Entry {

        private final String logId_;

        private final byte[] payload_;

        TestingEntry(String logId, byte[] payload) {
            logId_ = logId;
            payload_ = payload;
        }

        @Override
        public String getLogId() {
            return logId_;
        }

        @Override
        public byte[] getPayload() {
            return payload_;
        }
    }

    private class Executor implements Runnable {

        Random random_;

        private int id = 0;

        private final CyclicBarrier startingBarrier_;

        private final CyclicBarrier endingBarrier_;

        private final AtomicLong numOperationHasDone = new AtomicLong(0);

        private final AtomicLong numEnqueuePerformed = new AtomicLong(0);

        private final AtomicLong numDequeuePerformed = new AtomicLong(0);

        private final AtomicLong numEmptyDequeuePerformed = new AtomicLong(0);

        public Executor(int seed, int id, CyclicBarrier startingBarrier, CyclicBarrier endingBarrier) {
            random_ = new Random(seed);
            this.id = id;
            startingBarrier_ = startingBarrier;
            endingBarrier_ = endingBarrier;
        }

        @Override
        public void run() {

            try {
                startingBarrier_.await();
                while (!shouldStop) {
                    int roll = random_.nextInt(total_);

                    if (roll < percentagePrimaryEnqueue) {
                        doEnqueue(StoreType.PRIMARY);
                        numEnqueuePerformed.getAndIncrement();
                    } else if (roll < percentagePrimaryDequeue) {
                        doDequeue(StoreType.PRIMARY);
                        numDequeuePerformed.getAndIncrement();
                    } else if (roll < percentageSecondaryEnqueue) {
                        doEnqueue(StoreType.SECONDARY);
                        numEnqueuePerformed.getAndIncrement();
                    } else if (roll < percentageTertiaryEnqueue) {
                        doEnqueue(StoreType.TERTIARY);
                        numEnqueuePerformed.getAndIncrement();
                    } else {
                        doDequeue(StoreType.EMPTY);
                        numEmptyDequeuePerformed.getAndIncrement();
                    }
                    numOperationHasDone.getAndIncrement();
                }

            } catch (Throwable e) {
                e.printStackTrace();
                error_ = true;
                exceptions.add(e);
            } finally {
                try {
                    endingBarrier_.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        }

        public long getNumOperations() {
            return numOperationHasDone.get();
        }

        public long getNumEnqueuedOpertions() {
            return numEnqueuePerformed.get();
        }

        public long getNumDequeueOpertions() {
            return numDequeuePerformed.get();
        }

        public long getNumEmptyDequeuedOpertions() {
            return numEmptyDequeuePerformed.get();
        }

        public void reset() {
            numOperationHasDone.set(0);
            numEnqueuePerformed.set(0);
            numDequeuePerformed.set(0);
            numEmptyDequeuePerformed.set(0);
        }

        private void doEnqueue(StoreType type) throws SeqStoreException, InterruptedException {
            StoreId destinationId = getRandomStoreId(type);
            SeqStore<AckIdV3, BasicInflightInfo> store = manager_.getStore(destinationId);
            try {
                TestingEntry message = new TestingEntry("Executor" + id + ":" + largestStoreIds_.get(type),
                        payload);
                store.enqueue(message, -1, null);
                // System.out.println("enqueued: " + message.getLogId());
            } catch (SeqStoreException e) {
                error_ = true;
                exceptions.add(e);
                throw new IllegalStateException(e);
            }
        }

        private StoreId getRandomStoreId(StoreType type) {
            Integer sotreId = largestStoreIds_.get(type);
            assertNotNull(id);
            StoreId destinationId = new StoreIdImpl(getStoreNamePrefix(type), ""
                    + random_.nextInt(sotreId));
            return destinationId;
        }

        private void doDequeue(StoreType type) throws SeqStoreException {
            StoreId destinationId = getRandomStoreId(type);
            SeqStore<AckIdV3, BasicInflightInfo> store = manager_.getStore(destinationId);
            Integer numReader = numReaderMap_.get(destinationId);
            assertNotNull(numReader);
            String readerId = getReaderName(destinationId, random_.nextInt(numReader));
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.getReader(readerId);
            try {
                StoredEntry<AckIdV3> msg = reader.dequeue();
                if (msg != null) {
                    reader.ack(msg.getAckId());
                    // System.out.println("Dequeued: " + msg.getLogId());
                }
            } catch (SeqStoreException e) {
                error_ = true;
                exceptions.add(e);
                throw new IllegalStateException(e);
            }
        }
    }

    private void createBackLogs(long numMsg, SeqStore<AckIdV3, BasicInflightInfo> store) throws SeqStoreException, InterruptedException {
        if (numMsg == 0) {
            numMsg++;
        }
        for (int i = 0; i < numMsg; i++) {
            store.enqueue(new TestingEntry(store.getStoreName() + "Backlog" + i, payload), -1, null);
        }
    }

    private void basicSetup() {
        payload = new byte[payloadSize];
        Arrays.fill(payload, (byte) 0);
    }

    private void setup() throws IOException, InvalidConfigException, SeqStoreException {
        basicSetup();
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        try {
            manager_ = BDBStoreManager.createDefaultTestingStoreManager(
                    confProvider, con.getImmutableConfig(), new AlwaysIncreasingClock());
        } catch (SeqStoreInternalException e) {
            throw new IllegalStateException("fail to start ", e);
        }
        excutors_ = new ArrayList<Executor>();
        largestStoreIds_.put(StoreType.PRIMARY, 0);
        largestStoreIds_.put(StoreType.SECONDARY, 0);
        largestStoreIds_.put(StoreType.TERTIARY, 0);
        largestStoreIds_.put(StoreType.EMPTY_DEQUEUE, 0);
        largestStoreIds_.put(StoreType.EMPTY, 0);

    }

    private void shutDown() throws SeqStoreException {
        manager_.close();
    }

    private void createStores(int numStores, int numBackLogs, StoreType type) throws SeqStoreException,
            InterruptedException {
        Random random = new Random(0);
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setCleanerPeriod(10 * 1000 + random.nextInt(10 * 1000));
        storeConfig.setMaxMessageLifeTime(3000);
        storeConfig.setSharedDBBucketStoreConfig(
                new BucketStoreConfig.Builder()
                    .withMinPeriod(30)
                    .withMaxEnqueueWindow(300)
                    .withMaxPeriod(600)
                    .withMinSize(5)
                    .build() );
        storeConfig.setDedicatedDBBucketStoreConfig(
                new BucketStoreConfig.Builder()
                    .withMinPeriod(60)
                    .withMaxEnqueueWindow(30)
                    .withMaxPeriod(240)
                    .withMinSize(10)
                    .build() );
        storeConfig.setMaxSizeInKBBeforeUsingDedicatedDBBuckets(20);
        for (int i = 0; i < numStores; i++) {
            StoreIdImpl storeName = new StoreIdImpl(getStoreNamePrefix(type), "" + largestStoreIds_.get(type));
            confProvider.putStoreConfig(storeName.getGroupName(), storeConfig);
            manager_.createStore(storeName);
            SeqStoreInternalInterface<BasicInflightInfo> store = manager_.getStore(storeName);
            createBackLogs(numBackLogs, store);
            createReaders(numReaderPerStore, store);
            // dequeue the message out for those empty queues
            if (numBackLogs == 0) {
                for (int j = 0; j < numReaderPerStore; j++) {
                    String readerId = getReaderName(storeName, j);
                    SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.getReader(readerId);
                    StoredEntry<AckIdV3> msg = reader.dequeue();
                    reader.ack(msg.getAckId());
                }
            }
            System.out.println("Created store: " + storeName);
            Integer previous = largestStoreIds_.get(type);
            assertNotNull(previous);
            largestStoreIds_.replace(type, Integer.valueOf(previous + 1));
        }
    }

    private void createReaders(int numReaders, SeqStoreInternalInterface<BasicInflightInfo> store) throws SeqStoreException {
        numReaderMap_.put(store.getStoreId(), numReaders);
        for (int i = 0; i < numReaders; i++) {
            store.createReader(getReaderName(store.getStoreId(), i));
        }
    }

    private String getStoreNamePrefix(StoreType type) {
        return "store" + type;
    }

    private String getReaderName(StoreId destinationId, int num) {
        return destinationId + "reader" + num;
    }

    @Test
    public void realSQSTest() throws Throwable {
        String isCoveragePassStr = 
                System.getProperties().getProperty( "dse.RunAsCoverageTest" );
        if( isCoveragePassStr != null && (
                isCoveragePassStr.equalsIgnoreCase( "true") || 
                isCoveragePassStr.equalsIgnoreCase( "t" ) ) ) 
        {
            // Don't run for coverage tests
            return;
        }
        
        int storeFactor = 1;
        System.out.println("numStoreFactor: " + storeFactor);
        setup();
        loadTest(storeFactor, MessagesPerStore_);
        shutDown();
    }

    private void loadTest(int numStoreFactor, int numBacklog) throws Throwable {
        createStores(1 * numStoreFactor, numBacklog, StoreType.PRIMARY);
        createStores(5 * numStoreFactor, numBacklog, StoreType.SECONDARY);
        createStores(20 * numStoreFactor, numBacklog, StoreType.TERTIARY);
        createStores(9 * numStoreFactor, 0, StoreType.EMPTY_DEQUEUE);
        createStores(200 * numStoreFactor, 0, StoreType.EMPTY);
        System.out.println(235 * numStoreFactor + "stores have been created");
        System.out.println("Finished creating stores, Starting the test");
        doTest();
        System.gc();
        Thread.sleep(5000);
    }

    private void doTest() throws Throwable {
        shouldStop = false;
        CyclicBarrier startingBarrier = new CyclicBarrier(numThread_ + 1);
        CyclicBarrier endBarrier = new CyclicBarrier(numThread_ + 1);
        for (int i = 0; i < numThread_; i++) {
            Executor executor = new Executor(i, i, startingBarrier, endBarrier);
            excutors_.add(executor);
            Thread t = new Thread(executor);
            t.start();
        }
        startingBarrier.await();
        for (int i = 0; i < 1; i++) {
            long startTime = System.currentTimeMillis();
            Thread.sleep(20 * 1000);
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            long totalOperations = 0;
            long totalEnqueue = 0;
            long totalDequeue = 0;
            long totalEmptyDequeue = 0;
            for (Executor executor : excutors_) {
                totalOperations += executor.getNumOperations();
                totalEnqueue += executor.getNumEnqueuedOpertions();
                totalDequeue += executor.getNumDequeueOpertions();
                totalEmptyDequeue += executor.getNumEmptyDequeuedOpertions();
                executor.reset();
            }
            performanceHistory.put(totalTime, ((double) (totalOperations * 1000)) / totalTime);
            System.out.println("Throughput = " + ((double) totalOperations * 1000) / totalTime + "per sec");
            System.out.println("Memory usage: "
                    + (((double) (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())) / 1024 / 1024)
                    + " Megabytes");
            System.out.println((((double) totalEnqueue * 1000)) / totalTime);
            System.out.println((((double) totalDequeue * 1000)) / totalTime);
            System.out.println((((double) totalEmptyDequeue * 1000)) / totalTime);
            ((manager_)).printDBStats();
        }
        shouldStop = true;
        endBarrier.await();
        for (Throwable e : exceptions) {
            e.printStackTrace();
        }
        if (!exceptions.isEmpty())
            throw exceptions.peek();
        assertTrue(error_ == false);

        for (Map.Entry<Long, Double> e : performanceHistory.entrySet()) {
            System.out.println("Time: " + e.getKey() + "  Throughput: " + e.getValue());
        }
        System.out.println("Shutting down");
    }
}
