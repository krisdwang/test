package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreIllegalStateException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.util.BasicInflightInfo;

public abstract class StressTestV3Base {

    protected SeqStoreManager<AckIdV3, BasicInflightInfo> manager_;

    protected ArrayList<SeqStore<AckIdV3, BasicInflightInfo>> stores_ =
    	new ArrayList<SeqStore<AckIdV3, BasicInflightInfo>>();

    String payload;

    protected static final AckIdGenerator ackIdGen_ = new AckIdGenerator();

    private static final int payloadSize = 2048;

    private static final int numStores = 1;

    private boolean error = false;

    private static final int msgsPerEnqueuer = 5000;

    private static final int totalDequeueEmpty = 100000;

    private static final int numReaderPerStore = 5;

    private static final int numEnqueuerPerStore = 5;

    private static final int numThreadPerReader = 4;

    private static final int numMsgPerStore = msgsPerEnqueuer * numEnqueuerPerStore;

    class Enqueuer implements Runnable {

        AtomicInteger enqueueCount = new AtomicInteger(0);

        long startingTime = Long.MAX_VALUE;

        long endingTime = Long.MAX_VALUE;

        SeqStore<AckIdV3, BasicInflightInfo> store_;

        CyclicBarrier sbarrier_;

        CyclicBarrier ebarrier_;

        Enqueuer(SeqStore<AckIdV3, BasicInflightInfo> store, CyclicBarrier barrier, CyclicBarrier barrier2) {
            store_ = store;
            sbarrier_ = barrier;
            ebarrier_ = barrier2;
        }

        @Override
        public void run() {
            Entry message = new TestEntry(payload);
            try {
                sbarrier_.await();
                startingTime = System.currentTimeMillis();

                while (enqueueCount.getAndIncrement() < msgsPerEnqueuer) {
                    store_.enqueue(message, -1, null);
                }
                enqueueCount.decrementAndGet(); // Prevent adding one when
                                                // breaking out of the loop.
                endingTime = System.currentTimeMillis();
                ebarrier_.await();
            } catch (Throwable e) {
                error = true;
                throw new IllegalStateException("problem when enqueuing", e);
            }
        }

    }

    class Dequeuer implements Runnable {

        AtomicInteger dequeueCount = new AtomicInteger(0);

        SeqStoreReader<AckIdV3, BasicInflightInfo> reader_;

        CyclicBarrier barrier_;

        CyclicBarrier barrier2_;

        long startingTime = Long.MAX_VALUE;

        long endingTime = Long.MAX_VALUE;

        Dequeuer(SeqStoreReader<AckIdV3, BasicInflightInfo> reader, CyclicBarrier barrier, CyclicBarrier barrier2) {
            reader_ = reader;
            barrier_ = barrier;
            barrier2_ = barrier2;
        }

        @Override
        public void run() {
            try {
                barrier_.await();
                startingTime = System.currentTimeMillis();
                StoredEntry<AckIdV3> entry;
                while (dequeueCount.get() < msgsPerEnqueuer) {
                    if ((entry = reader_.dequeue()) != null) {
                        reader_.ack(entry.getAckId());
                        dequeueCount.getAndIncrement();
                    } else {
                        break;
                    }
                }
                endingTime = System.currentTimeMillis();
                barrier2_.await();
            } catch (Throwable e) {
                error = true;
                throw new IllegalStateException("problem when dequeuing", e);
            }
        }

    }

    class StressDequeuer implements Runnable {

        AtomicInteger dequeueCount = new AtomicInteger(0);

        class MyThread implements Runnable {

            @Override
            public void run() {
                try {
                    barrier_.await();
                    while (dequeueCount.get() < numMsgPerStore) {
                        StoredEntry<AckIdV3> entry;
                        if ((entry = reader_.dequeue()) != null) {
                            dequeueCount.getAndIncrement();
                            reader_.ack(entry.getAckId());
                        }
                    }
                    System.out.println(Thread.currentThread().getName() + "dequeued:" + dequeueCount.get());
                    barrier2_.await();
                } catch (Throwable e) {
                    error = true;
                    throw new IllegalStateException("problem when dequeuing", e);
                }

            }

        }

        SeqStoreReader<AckIdV3, BasicInflightInfo> reader_;

        CyclicBarrier barrier_;

        CyclicBarrier barrier2_;

        StressDequeuer(SeqStoreReader<AckIdV3, BasicInflightInfo> reader, CyclicBarrier barrier, CyclicBarrier barrier2) {
            reader_ = reader;
            barrier_ = barrier;
            barrier2_ = barrier2;
        }

        @Override
        public void run() {
            try {
                ArrayList<MyThread> myThreads = new ArrayList<MyThread>();
                for (int i = 0; i < numThreadPerReader; i++) {
                    MyThread mt = new MyThread();
                    myThreads.add(mt);
                    Thread t = new Thread(mt);
                    t.start();
                }

            } catch (Exception e) {
                error = true;
                throw new IllegalStateException("problem when dequeuing", e);
            }
        }

    }

    class ThroughputDequeuer implements Runnable {

        AtomicInteger dequeueCount = new AtomicInteger(0);

        SeqStoreReader<AckIdV3, BasicInflightInfo> reader_;

        CyclicBarrier barrier_;

        CyclicBarrier barrier2_;

        long startingTime = Long.MAX_VALUE;

        long endingTime = Long.MAX_VALUE;

        ThroughputDequeuer(SeqStoreReader<AckIdV3, BasicInflightInfo> reader, CyclicBarrier barrier, CyclicBarrier barrier2) {
            reader_ = reader;
            barrier_ = barrier;
            barrier2_ = barrier2;
        }

        @Override
        public void run() {
            try {
                barrier_.await();
                startingTime = System.currentTimeMillis();
                StoredEntry<AckIdV3> entry;
                while (dequeueCount.get() < msgsPerEnqueuer) {
                    if ((entry = reader_.dequeue()) != null) {
                        dequeueCount.getAndIncrement();
                        reader_.ack(entry.getAckId());
                    }
                }
                endingTime = System.currentTimeMillis();
                barrier2_.await();
            } catch (Throwable e) {
                error = true;
                throw new IllegalStateException("problem when dequeuing", e);
            }
        }

    }

    class DequeuerForEmptyQueue implements Runnable {

        AtomicInteger dequeueCount = new AtomicInteger(0);

        SeqStoreReader<AckIdV3, BasicInflightInfo> reader_;

        CyclicBarrier barrier_;

        CyclicBarrier barrier2_;

        long startingTime = Long.MAX_VALUE;

        long endingTime = Long.MAX_VALUE;

        DequeuerForEmptyQueue(SeqStoreReader<AckIdV3, BasicInflightInfo> reader, CyclicBarrier barrier, CyclicBarrier barrier2) {
            reader_ = reader;
            barrier_ = barrier;
            barrier2_ = barrier2;
        }

        @Override
        public void run() {
            try {
                barrier_.await();
                startingTime = System.currentTimeMillis();
                while (dequeueCount.get() < totalDequeueEmpty) {
                    if (reader_.dequeue() != null)
                        throw new IllegalStateException();
                    dequeueCount.getAndIncrement();
                }
                endingTime = System.currentTimeMillis();
                barrier2_.await();
            } catch (Throwable e) {
                error = true;
                throw new IllegalStateException("problem when dequeuing", e);
            }
        }

    }

    public void basicSetup() {
        StringBuffer payloadB = new StringBuffer();
        for (int i = 0; i < payloadSize; i++) {
            payloadB.append('a');
        }
        payload = payloadB.toString();
    }

    protected abstract void setUp() throws IOException, InvalidConfigException, SeqStoreException;

    protected abstract void shutdown() throws SeqStoreIllegalStateException, SeqStoreException;

    @Test
    public void testLotsOfStoresOneEnqueuerPerStore() throws SeqStoreException {
        for (int j = 0; j < 2; j++) {
            try {

                System.out.println("\nStarting testLotsOfStoresOneEnqueuerPerStore");
                setUp();
                ArrayList<Enqueuer> enqueuers = new ArrayList<Enqueuer>();
                CyclicBarrier barrier = new CyclicBarrier(numStores);
                CyclicBarrier barrier2 = new CyclicBarrier(numStores + 1);
                ArrayList<Thread> threads = new ArrayList<Thread>();
                createStores();

                for (SeqStore<AckIdV3, BasicInflightInfo> store : stores_) {
                    Enqueuer e = new Enqueuer(store, barrier, barrier2);
                    enqueuers.add(e);
                    Thread t = new Thread(e);
                    threads.add(t);
                }

                for (Thread t : threads) {
                    t.start();
                }
                barrier2.await();

                int sum = 0;
                for (Enqueuer e : enqueuers) {
                    double thoughPut = ((double) (msgsPerEnqueuer) * 1000) / (e.endingTime - e.startingTime);
                    System.out.println("The enqueue thoughput for testLotsOfStoresOneEnqueuerPerStore is "
                            + thoughPut);
                    if (j == 1) {
                        assertTrue(thoughPut > 500);
                    }
                    // System.out.println("Actual enqueued" +
                    // e.enqueueCount.get());
                    sum = sum + e.enqueueCount.get();
                }
                assertEquals(msgsPerEnqueuer * numStores, sum);

            } catch (Throwable e) {
                error = true;
                throw new IllegalStateException(e);

            } finally {
                shutdown();
            }
            assertFalse(error);
        }
    }

    @Test
    public void testLotsStoresOneDequeuerPerStore() throws SeqStoreException, Exception {
        System.out.println("\nStarting testLotsStoresOneDequeuerPerStore");
        for (int j = 0; j < 2; j++) {
            try {
                setUp();
                CyclicBarrier barrier = new CyclicBarrier(numStores);
                CyclicBarrier barrier2 = new CyclicBarrier(numStores + 1);
                ArrayList<Dequeuer> dequeuers = new ArrayList<Dequeuer>();
                ArrayList<Thread> threads = new ArrayList<Thread>();
                createStores();

                for (SeqStore<AckIdV3, BasicInflightInfo> store : stores_) {
                    Enqueuer e = new Enqueuer(store, barrier, barrier2);
                    Thread t = new Thread(e);
                    threads.add(t);
                }

                for (Thread t : threads) {
                    t.start();
                }
                barrier2.await();

                barrier = new CyclicBarrier(numStores);
                barrier2 = new CyclicBarrier(numStores + 1);
                threads.clear();
                int i = 0;
                for (SeqStore<AckIdV3, BasicInflightInfo> store : stores_) {

                    store.createReader("reader" + i);
                    Dequeuer d = new Dequeuer(store.getReader("reader" + i), barrier, barrier2);
                    Thread t = new Thread(d);
                    dequeuers.add(d);
                    threads.add(t);
                    i++;
                }

                for (Thread t : threads) {
                    t.start();
                }
                barrier2.await();
                int sum = 0;
                for (Dequeuer e : dequeuers) {
                    double thoughPut = ((double) (1000 * msgsPerEnqueuer)) / (e.endingTime - e.startingTime);
                    if (j == 1) {
                        assertTrue(thoughPut > 1000);
                    }
                    System.out.println("The dequeue thoughput for testLotsOfStoresOneDequeuerPerStore is "
                            + thoughPut);
                    // System.out.println("Actual dequeued" +
                    // e.dequeueCount.get());
                    sum = sum + e.dequeueCount.get();
                }
                assertEquals(msgsPerEnqueuer * numStores, sum);
            } finally {
                shutdown();
            }
            assertFalse(error);
        }
    }

    @Test
    public void testLotsStoresThoughput() throws SeqStoreException, Exception {
        System.out.println("\nStarting testLotsStoresThoughput");
        for (int j = 0; j < 2; j++) {
            try {

                setUp();
                CyclicBarrier barrier = new CyclicBarrier(numStores * 2);
                CyclicBarrier barrier2 = new CyclicBarrier(numStores * 2 + 1);
                ArrayList<ThroughputDequeuer> dequeuers = new ArrayList<ThroughputDequeuer>();
                ArrayList<Thread> threads = new ArrayList<Thread>();
                createStores();

                for (SeqStore<AckIdV3, BasicInflightInfo> store : stores_) {
                    Enqueuer e = new Enqueuer(store, barrier, barrier2);
                    Thread t = new Thread(e);
                    threads.add(t);
                }

                for (SeqStore<AckIdV3, BasicInflightInfo> store : stores_) {
                    store.createReader("reader");
                    ThroughputDequeuer d = new ThroughputDequeuer(store.getReader("reader"), barrier, barrier2);
                    Thread t = new Thread(d);
                    dequeuers.add(d);
                    threads.add(t);
                }

                for (Thread t : threads) {
                    t.start();
                }
                barrier2.await();
                int sum = 0;
                for (ThroughputDequeuer e : dequeuers) {
                    double thoughPut = ((double) (1000 * e.dequeueCount.get()))
                            / (e.endingTime - e.startingTime);
                    if (j == 1) {
                        assertTrue(thoughPut > 500);
                    }
                    System.out.println("The dequeue thoughput for testLotsOfStoresThoughput is " + thoughPut);
                    sum = sum + e.dequeueCount.get();
                }
                assertEquals(msgsPerEnqueuer * numStores, sum);
            } finally {
                shutdown();
            }
            assertFalse(error);
        }
    }

    @Test
    public void testDequeueEmptyQueue() throws SeqStoreException, Exception {
        for (int j = 0; j < 2; j++) {
            try {
                setUp();
                CyclicBarrier barrier = new CyclicBarrier(numStores);
                CyclicBarrier barrier2 = new CyclicBarrier(numStores + 1);
                ArrayList<DequeuerForEmptyQueue> dequeuers = new ArrayList<DequeuerForEmptyQueue>();
                ArrayList<Thread> threads = new ArrayList<Thread>();
                createStores();
                int i = 0;
                for (SeqStore<AckIdV3, BasicInflightInfo> store : stores_) {

                    store.createReader("reader" + i);
                    DequeuerForEmptyQueue d = new DequeuerForEmptyQueue(store.getReader("reader" + i), barrier, barrier2);
                    Thread t = new Thread(d);
                    dequeuers.add(d);
                    threads.add(t);
                    i++;
                }

                for (Thread t : threads) {
                    t.start();
                }
                barrier2.await();
                int sum = 0;
                for (DequeuerForEmptyQueue e : dequeuers) {
                    double thoughPut = ((double) (1000 * totalDequeueEmpty))
                            / (e.endingTime - e.startingTime);
                    System.out.println("The dequeue thoughput for testLotsOfStoresOneDequeuerPerStore is "
                            + thoughPut);
                    if (j == 1) {
                        assertTrue(thoughPut > 10000);
                    }
                    sum = sum + e.dequeueCount.get();
                }
                assertEquals(numStores * totalDequeueEmpty, sum);
                shutdown();
            } finally {
                shutdown();
            }
            assertFalse(error);
        }
    }

    @Test
    public void testLotsStoresManyReadersPerStoreManyThreadsPerReader() throws SeqStoreException, Exception {
        try {
            setUp();
            int numThreads = numStores * (numReaderPerStore * numThreadPerReader + numEnqueuerPerStore);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            CyclicBarrier barrier2 = new CyclicBarrier(numThreads + 1);
            ArrayList<StressDequeuer> dequeuers = new ArrayList<StressDequeuer>();
            createStores();

            for (SeqStore<AckIdV3, BasicInflightInfo> store : stores_) {
                for (int i = 0; i < numEnqueuerPerStore; i++) {
                    Enqueuer e = new Enqueuer(store, barrier, barrier2);
                    Thread t = new Thread(e);
                    t.start();
                }
            }
            ArrayList<SeqStoreReader<AckIdV3, BasicInflightInfo>> readers = new ArrayList<SeqStoreReader<AckIdV3, BasicInflightInfo>>();
            for (SeqStore<AckIdV3, BasicInflightInfo> store : stores_) {
                for (int i = 0; i < numReaderPerStore; i++) {
                    store.createReader("reader" + i);
                    SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.getReader("reader" + i);
                    readers.add(reader);
                    StressDequeuer d = new StressDequeuer(reader, barrier, barrier2);
                    Thread t = new Thread(d);
                    dequeuers.add(d);
                    t.start();
                }
            }
            barrier2.await();

            for (SeqStoreReader<AckIdV3, BasicInflightInfo> reader : readers) {
                assertEquals(null, reader.dequeue());
            }
            int sum = 0;
            for (StressDequeuer d : dequeuers) {
                sum += d.dequeueCount.get();
            }
            assertEquals(numStores * numEnqueuerPerStore * msgsPerEnqueuer * numReaderPerStore, sum);
        } finally {
            shutdown();
        }
        assertFalse(error);
    }

    private void createStores() throws SeqStoreException {
        for (int i = 0; i < numStores; i++) {
            StoreIdImpl storeId = new StoreIdImpl("store", "" + i);
            manager_.createStore(storeId);
            stores_.add(manager_.getStore(storeId));
        }
    }

}
