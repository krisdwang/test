package com.amazon.messaging.seqstore.v3.bdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public class BDBPersistenceTest extends TestCase {
    
    private static final Log log = LogFactory.getLog(BDBPersistenceTest.class);

    SeqStoreManager<AckIdV3, BasicInflightInfo> manager_ = null;

    ArrayList<SeqStore<AckIdV3, BasicInflightInfo>> stores_ =
    	new ArrayList<SeqStore<AckIdV3, BasicInflightInfo>>();

    ArrayList<ConcurrentSkipListSet<String>> outStandingMessages = new ArrayList<ConcurrentSkipListSet<String>>();
    
    String payload = null;

    protected static final AckIdGenerator ackIdGen_ = new AckIdGenerator();

    private static final int payloadSize = 2048;

    private static final int numStores = 15;

    private final BasicConfigProvider<BasicInflightInfo> confProvider =
    	BasicConfigProvider.newBasicInflightConfigProvider();

    public static class TestingEntry extends Entry {

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
        @SuppressWarnings("EI_EXPOSE_REP")
        public byte[] getPayload() {
            return payload_;
        }
    }

    class Enqueuer implements Runnable {

        private final String enqueuerName_;

        SeqStore<AckIdV3, BasicInflightInfo> store_;

        CyclicBarrier sbarrier_;

        CyclicBarrier ebarrier_;

        private final ConcurrentSkipListSet<String> messages;

        Enqueuer(SeqStore<AckIdV3, BasicInflightInfo> store, CyclicBarrier barrier, CyclicBarrier barrier2, ConcurrentSkipListSet<String> messages, String name) {
            store_ = store;
            sbarrier_ = barrier;
            ebarrier_ = barrier2;
            this.messages = messages;
            enqueuerName_ = name;
        }

        @Override
        public void run() {
            StoredEntryV3 message = null;
            try {
                sbarrier_.await();
                int numMsg = 0;
                while (true) {
                    message = new StoredEntryV3(ackIdGen_.next( System.currentTimeMillis() ), payload.getBytes(),enqueuerName_ + ":" + numMsg);
                    messages.add(message.getLogId());
                    store_.enqueue(message, -1, null);
                    numMsg++;
                }

            } catch (SeqStoreClosedException e) {
                assert message != null;
                messages.remove(message.getLogId()); // The last one failed.
                log.info("The store has been closed, stopping enqueues.");
            } catch (Exception e) {
                throw new IllegalStateException("problem when enqueuing", e);
            } finally {
                try {
                    ebarrier_.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    static class ThroughputDequeuer implements Runnable {

        SeqStoreReader<AckIdV3, BasicInflightInfo> reader_;

        CyclicBarrier barrier_;

        CyclicBarrier barrier2_;

        private final ConcurrentSkipListSet<String> messages;

        ThroughputDequeuer(SeqStoreReader<AckIdV3, BasicInflightInfo> reader, CyclicBarrier barrier, CyclicBarrier barrier2,
                ConcurrentSkipListSet<String> messages) {
            reader_ = reader;
            barrier_ = barrier;
            barrier2_ = barrier2;
            this.messages = messages;
        }

        @Override
        public void run() {
            try {
                barrier_.await();
                StoredEntry<AckIdV3> entry = reader_.dequeue();
                while (entry != null) {
                    boolean acked = reader_.ack(entry.getAckId());
                    assertTrue(acked);
                    boolean removed = messages.remove(entry.getLogId());
                    assertTrue(removed);
                    entry = reader_.dequeue();
                }
            } catch (SeqStoreClosedException e) {
                log.info("The store has been closed, stopping dequeues.");
            } catch (Exception e) {
                throw new IllegalStateException("problem when dequeuing", e);
            } finally {
                try {
                    barrier2_.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    protected void createManager(boolean clearDisk) throws IOException, InvalidConfigException, SeqStoreException {
        StringBuffer payloadB = new StringBuffer();
        for (int i = 0; i < payloadSize; i++) {
            payloadB.append('a');
        }
        payload = payloadB.toString();
        stores_.clear();
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        File bdbdir;
        if (clearDisk)
            bdbdir = SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName());
        else
            bdbdir = SeqStoreTestUtils.getBDBDir(getClass().getSimpleName());
        
        con.setStoreDirectory(bdbdir);
        try {
            manager_ = BDBStoreManager.createDefaultTestingStoreManager(
                    confProvider, con.getImmutableConfig(), new AlwaysIncreasingClock() );
        } catch (SeqStoreInternalException e) {
            throw new IllegalStateException("fail to start", e);
        }

    }

    @After
    public void tearDown() throws SeqStoreException {
        manager_.close();
    }


    @Test
    public void testShutdownWhenEnqueueDequeue() throws SeqStoreException, Exception {
        createManager(true);
        int numEnqueuer = 3;
        CyclicBarrier startingBarrier = new CyclicBarrier(numStores * numEnqueuer + numStores);
        CyclicBarrier endingBarrier = new CyclicBarrier(numStores * numEnqueuer + numStores + 1);
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setCleanerPeriod(Integer.MAX_VALUE);
        ArrayList<ThroughputDequeuer> dequeuers = new ArrayList<ThroughputDequeuer>();
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < numStores; i++) {
            SeqStore<AckIdV3, BasicInflightInfo> store = createStore(storeConfig, i);
            
            ConcurrentSkipListSet<String> messages = new ConcurrentSkipListSet<String>();
            outStandingMessages.add(messages);
           
            createDequeuer(startingBarrier, endingBarrier, dequeuers, threads, store, messages);
            
            for (int j = 0; j < numEnqueuer; j++) {
                Enqueuer e = new Enqueuer(store, startingBarrier, endingBarrier, messages, store.getStoreName()
                        + "enqueuer" + j);
                Thread t = new Thread(e);
                t.setName(store.getStoreName() + "Enqueuer" + j);
                threads.add(t);
            }
        }

        assertEquals(numStores, stores_.size());
        assertEquals(startingBarrier.getParties(),threads.size());
        assertEquals(endingBarrier.getParties(),threads.size()+1);
        
        for (Thread t : threads) {
            t.start();
        }
        
        Thread.sleep(3000);
        
        manager_.close();
        
        endingBarrier.await();
        assertEquals(numStores, stores_.size());
        
        createManager(false);
        dequeuers = new ArrayList<ThroughputDequeuer>();
        threads = new ArrayList<Thread>();
        
        startingBarrier = new CyclicBarrier(numStores);
        endingBarrier = new CyclicBarrier(numStores + 1);
        
        for (int i = 0; i < numStores; i++) {
            SeqStore<AckIdV3, BasicInflightInfo> store = createStore(storeConfig, i);
            
            createDequeuer(startingBarrier, endingBarrier, dequeuers, threads, store, outStandingMessages.get(i));
        }
        
        for (Thread t : threads) {
            t.start();
        }
        endingBarrier.await();
        assertEquals(outStandingMessages.size(), stores_.size());
        for (ConcurrentSkipListSet<String> msgs : outStandingMessages) {
            assertTrue("Failed to dequeue message: "+ msgs.toString(),msgs.isEmpty());
        }
    }

    private SeqStore<AckIdV3, BasicInflightInfo> createStore(SeqStoreConfig storeConfig, int i) throws SeqStoreException {
        StoreId id = new StoreIdImpl("store", "" + i);
        confProvider.putStoreConfig(id.getGroupName(), storeConfig);
        manager_.createStore(id);
        SeqStore<AckIdV3, BasicInflightInfo> store = manager_.getStore(id);
        stores_.add(store);
        store.createReader("reader");
        return store;
    }

    private void createDequeuer(CyclicBarrier startingBarrier, CyclicBarrier endingBarrier,
            ArrayList<ThroughputDequeuer> dequeuers, ArrayList<Thread> threads,
            SeqStore<AckIdV3, BasicInflightInfo> store, ConcurrentSkipListSet<String> messages)
        throws SeqStoreException 
    {
        ThroughputDequeuer d = new ThroughputDequeuer(store.getReader("reader"), startingBarrier,
                endingBarrier, messages);
        Thread t = new Thread(d);
        t.setName(store.getStoreName() + "Dequeuer");
        dequeuers.add(d);
        threads.add(t);
    }
}
