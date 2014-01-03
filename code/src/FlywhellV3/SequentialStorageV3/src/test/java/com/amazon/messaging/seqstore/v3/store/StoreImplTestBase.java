package com.amazon.messaging.seqstore.v3.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.CyclicBarrier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.testing.TestCase;

public abstract class StoreImplTestBase extends TestCase {

    protected static final int MAX_MSG = 1000;

    protected static final int MAX_DEQ_TIME = 1000;

    protected static final long INTERVAL = 5000;
    
    protected static final SeqStoreImmutableConfig storeConfig;
    
    static {
        assertEquals( "INTERVAL must be a multiple of 1000", 0, INTERVAL % 1000 );
        
        BucketStoreConfig bucketStoreConfiguration 
            = new BucketStoreConfig.Builder()
                .withMaxEnqueueWindow(0)
                .withMaxPeriod((int) (INTERVAL/1000) )
                .withMinPeriod((int) (INTERVAL/1000) )
                .withMinSize(0)
                .build();
        
        SeqStoreConfig tmpConfig = new SeqStoreConfig();
        tmpConfig.setSharedDBBucketStoreConfig(bucketStoreConfiguration); // not used
        tmpConfig.setDedicatedDBBucketStoreConfig(bucketStoreConfiguration);
        
        storeConfig = tmpConfig.getImmutableConfig();
    }

    protected Random random_;

    Vector<AckIdV3> ids_;

    Vector<byte[]> payloads_;

    protected int numMsg_;

    protected AckIdV3 maxId_;

    protected AckIdV3 minId_;

    protected AckIdGenerator ackIdGen;

    protected Store store_;

    protected SettableClock clock_;

    protected CyclicBarrier barrier_;

    int numReaders_ = 100;

    int numEnqueuers_ = 100;

    protected Throwable e_;

    protected class Reader implements MyRunnable {

        protected StoreIterator iter;

        protected boolean shouldStop_;

        Reader() throws SeqStoreException {
            iter = store_.getIterAt(ackIdGen.getAckId(0));
            shouldStop_ = false;
        }

        @Override
        public void stop() {
            shouldStop_ = true;
        }

        @Override
        public void run() {
            try {
                barrier_.await();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

            while (!shouldStop_) {
                StoredEntry<AckIdV3> next;
                try {
                    next = iter.next();
                } catch (SeqStoreException e) {
                    throw new RuntimeException( "Failed getting message: " + e.getMessage(), e );
                }
                if ((next != null) && (store_.getLastAvailableId() != null)) {
                    assertTrue(next.getAckId().compareTo(store_.getLastAvailableId()) <= 0);
                }
            }
        }

    }

    protected class Enqueuer implements MyRunnable {

        protected boolean shouldStop_;

        Enqueuer() {
            shouldStop_ = false;
        }

        @Override
        public void stop() {
            shouldStop_ = true;
        }

        @Override
        public void run() {
            try {
                barrier_.await();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            while (!shouldStop_) {
                long enqueueTime = System.currentTimeMillis() + random_.nextInt(100000);
                try {
                    createAndEnqueueMsg(enqueueTime);
                } catch (Exception e) {
                    throw new IllegalStateException();
                }
            }
        }

    }

    protected class Cleaner implements MyRunnable {

        protected volatile AckIdV3 level_;

        protected volatile boolean shouldStop_;

        Cleaner() {
            level_ = null;
            shouldStop_ = false;
        }

        @Override
        public void stop() {
            shouldStop_ = true;
        }

        public void setLevel(AckIdV3 ackLevel) {
            level_ = ackLevel;
        }

        @Override
        public void run() {
            try {
                barrier_.await();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            while (!shouldStop_) {
                if (level_ != null) {
                    try {
                        store_.deleteUpTo(level_);
                    } catch (SeqStoreException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
    }

    protected class ThrowToJunitHandler implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            e.printStackTrace();
            e_ = e;
        }
    }

    protected interface MyRunnable extends Runnable {
        public void stop();
    }

    @Before
    abstract protected void setup() throws Exception;

    @After
    abstract public void shutdown() throws Exception;

    @Test
    public void testStoreIterator() throws Exception {
        clock_.setCurrentTime(1);
        AckIdV3 first = createAndEnqueueMsg(100);
        
        StoreIterator iter = store_.getIterAt(ackIdGen.getAckId(0));
        
        // The message isn't available yet
        assertNull( iter.next() );
        
        // increase time so the message is available
        clock_.setCurrentTime(100);
        assertEquals(first, iter.next().getAckId());
        
        TreeSet<AckIdV3> messages = new TreeSet<AckIdV3>();
        
        // test can deal with multiple buckets and enqueue after dequeue
        messages.add( createAndEnqueueMsg(INTERVAL + 2001) );
        messages.add( createAndEnqueueMsg(INTERVAL + 2002) );
        messages.add( createAndEnqueueMsg(INTERVAL + 1000) );
        messages.add( createAndEnqueueMsg(INTERVAL + 1001) );
        messages.add( createAndEnqueueMsg(INTERVAL + 1062) );
        assertEquals( 2, store_.getNumBuckets() );
        
        Iterator<AckIdV3> treeSetItr = messages.iterator();
        // nothing is available yet
        assertNull( iter.next() );
        
        // Make available 1000, 1001
        clock_.setCurrentTime(INTERVAL + 1020);
        assertEquals(treeSetItr.next(), iter.next().getAckId() ); // 1000
        // Make 1062 available 
        clock_.setCurrentTime(INTERVAL + 2000);
        assertEquals(treeSetItr.next(), iter.next().getAckId() ); // 1001
        assertEquals(treeSetItr.next(), iter.next().getAckId() ); //  1062
        assertNull( iter.next() ); // Nothing left
        // Make 2001, 2002 available
        clock_.setCurrentTime(INTERVAL + 2003);
        assertEquals(treeSetItr.next(), iter.next().getAckId() ); //  2001
        assertEquals(treeSetItr.next(), iter.next().getAckId() ); //  2002
        assertNull( iter.next() ); // Nothing left
    }

    @Test
    public void testDeletion() throws Exception {
        clock_.setCurrentTime(0);
        StoreIterator iter = store_.getIterAt(ackIdGen.getAckId(0));
        // create 15 random msg in the first bucket
        AckIdV3 firstMessage = createAndEnqueueMsgs(15, 1, INTERVAL).first();
        // And one message in the second bucket
        AckIdV3 message16 = createAndEnqueueMsg(INTERVAL + 1);
        assertEquals( 2, store_.getNumBuckets() );
        
        assertNull( iter.next() );
        // Make the first 15 messages available
        clock_.setCurrentTime(INTERVAL);
        assertEquals( firstMessage, iter.next().getAckId() );
        // Enqueue messages into the 3rd bucket
        AckIdV3 message17 = createAndEnqueueMsg(2 * INTERVAL);
        AckIdV3 message18 = createAndEnqueueMsg(2 * INTERVAL + 1);
        AckIdV3 message19 = createAndEnqueueMsg(2 * INTERVAL + 2);
        // Make the 16th message (the one in the second bucket available)
        clock_.setCurrentTime(2 * INTERVAL - 1);
        // Jump the iterator forward to just before the 16th message
        iter.advanceTo(new AckIdV3(INTERVAL, false));
        assertEquals( message16, iter.next().getAckId() );
        assertNull( iter.next() );
       
        AckIdV3 deleteLevel = new AckIdV3(INTERVAL + 1, true);
        // Delete messages up to and including message 16 - however message 16 will remain on disk
        //  because the current time hasn't left the second bucket so it could still take new messages
        int numDeleted = store_.deleteUpTo(deleteLevel);
        assertEquals(15, numDeleted);
        
        // No message should be available
        assertNull( iter.next() );
        
        // A new iterator shouldn't see message 16 as its been deleted even if it is still on disk
        iter.close();
        iter = store_.getIterAt(AckIdV3.MINIMUM);
        assertNull( iter.next() );
        
        // Make the 17th message available
        clock_.setCurrentTime(2 * INTERVAL);
        // The same ack level as before should now drop the second bucket as now
        //  no new messages can be enqueued to the bucket
        numDeleted = store_.deleteUpTo(deleteLevel);
        assertEquals(1, numDeleted);

        // And the iterator should move to the third bucket even though it was still in the deleted second bucket
        assertEquals( message17, iter.next().getAckId() );
        assertNull( iter.next() );
        
        // Make all the messages available
        clock_.setCurrentTime(2 * INTERVAL + 2);
        assertEquals( message18, iter.next().getAckId() );
        assertEquals( message19, iter.next().getAckId() );
        
        AckIdV3 message20 = createAndEnqueueMsg(INTERVAL * 2 + 2);
        assertEquals( message20, iter.next().getAckId() );
        // Move the clock out of the third bucket
        clock_.setCurrentTime(INTERVAL * 3);
        // Deleting below the second bucket shouldn't do anything
        numDeleted = store_.deleteUpTo(new AckIdV3( 2 * INTERVAL, false ) );
        assertEquals(0, numDeleted);
        
        // Enqueue a message into the fourth bucket
        AckIdV3 message21 = createAndEnqueueMsg(INTERVAL * 3);
        assertEquals( message21, iter.next().getAckId() );
        
        // Delete everything in the third bucket
        numDeleted = store_.deleteUpTo( new AckIdV3( message20, true ));
        assertEquals(4, numDeleted);
    }

    @Test
    public void enqueueDeleteImmediateAvailMsg() throws Exception {
        clock_.setCurrentTime(1);
        List<AckIdV3> ackIds = new ArrayList<AckIdV3>();
        for (int i = 0; i < 10; i++) {
            Entry entry = getStringEntry("message - test");
            AckIdV3 ackId = store_.getAckIdForEnqueue(1);
            try {
                store_.enqueue(ackId, entry, true, null);
            } finally {
                store_.enqueueFinished(ackId);
            }
            ackIds.add(ackId);
        }

        StoreIterator iter = store_.getIterAt(AckIdV3.MINIMUM);
        for( AckIdV3 ackId : ackIds ) {
            assertEquals( ackId, iter.next().getAckId() );
        }
        iter.close();
        
        // The clock is still in the store so none of them can be actually deleted
        assertEquals(0, store_.deleteUpTo( new AckIdV3( ackIds.get(9), true ) ) );
        
        iter = store_.getIterAt(AckIdV3.MINIMUM);
        assertNull( iter.next() ); // Nothing is available as the delete level has been moved passed all of them
        
        AckIdV3 lastId = store_.getAckIdForEnqueue(1);
        try {
            Entry entry = getStringEntry("message - test");
            store_.enqueue(lastId, entry, true, null);
        } finally {
            store_.enqueueFinished(lastId);
        }
        assertEquals( lastId, iter.next().getAckId() );
        assertNull( iter.next() );
        iter.close();
        
        // Still not deletable
        assertEquals(0, store_.deleteUpTo( new AckIdV3( lastId, true ) ) );
        
        clock_.setCurrentTime(INTERVAL);
        // And now everything should be deletable as the clock is no longer in the bucket
        assertEquals(11, store_.deleteUpTo( new AckIdV3( lastId, true ) ) );
    }

    @Test
    public void testReadingWhenDeleting() throws Throwable {
        Vector<MyRunnable> threads = new Vector<MyRunnable>();
        Vector<Thread> threads1 = new Vector<Thread>();
        for (int i = 0; i < numReaders_; i++) {
            Reader r = new Reader();
            Thread t1 = new Thread(r);
            threads1.add(t1);
            t1.setUncaughtExceptionHandler(new ThrowToJunitHandler());
            t1.start();
            Enqueuer eq = new Enqueuer();
            Thread t2 = new Thread(eq);
            threads1.add(t2);
            t2.setUncaughtExceptionHandler(new ThrowToJunitHandler());
            t2.start();

            threads.add(r);
            threads.add(eq);
        }
        Cleaner c = new Cleaner();
        Thread t3 = new Thread(c);
        threads1.add(t3);
        t3.setUncaughtExceptionHandler(new ThrowToJunitHandler());
        t3.start();
        threads.add(c);
        long time = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < time) {
            try {
                Thread.sleep(10);
            } catch (Exception e) {
                fail("Interrupted");
            }
            clock_.setCurrentTime(System.currentTimeMillis());
            if (store_.getLastAvailableId() != null) {
                c.setLevel(ackIdGen.next(store_.getLastAvailableId().getTime() - 1));
            }
        }

        for (int i = 0; i < numReaders_ * 2 + 1; i++) {
            threads.get(i).stop();
        }
        for (int i = 0; i < threads1.size(); i++) {
            threads1.get(i).join();
        }
        if (e_ != null) {
            throw e_;
        }
    }

    public StoreImplTestBase() {
        super();
    }

    @Test
    public void testAdvanceTo() throws Exception {
        clock_.setCurrentTime(0);
        createAndEnqueueMsgs(10, 1, INTERVAL - 1);
        createAndEnqueueMsg(INTERVAL + 2);
        clock_.setCurrentTime(INTERVAL + 10);

        StoreIterator iter = store_.getIterAt(ackIdGen.getAckId(0));
        AckIdV3 testId = iter.advanceTo(ackIdGen.next(INTERVAL + 3));
        assertTrue(testId.getTime() == INTERVAL + 2);
    }

    @Test
    public void testLastAvailableNextMsgTime() throws Exception {
        clock_.setCurrentTime(INTERVAL);
        StoreIterator iter = store_.getIterAt(ackIdGen.getAckId(0));
        long test1 = store_.getTimeOfNextMessage(iter.getCurKey()) ;
        assertEquals(Long.MAX_VALUE, test1);
        createAndEnqueueMsg(2*INTERVAL);
        createAndEnqueueMsg(2*INTERVAL + 1);
        createAndEnqueueMsg(2*INTERVAL + 5);
        createAndEnqueueMsg(2*INTERVAL + 20);
        test1 = store_.getTimeOfNextMessage(iter.getCurKey()) - clock_.getCurrentTime();
        assertEquals(INTERVAL, test1);

        clock_.setCurrentTime(2*INTERVAL);
        test1 = store_.getTimeOfNextMessage(iter.getCurKey()) - clock_.getCurrentTime();
        assertTrue(test1 <= 0);
        iter.next();

        test1 = store_.getTimeOfNextMessage(iter.getCurKey()) - clock_.getCurrentTime();
        assertEquals(test1, 1l);

        // test the next message should be available now
        clock_.setCurrentTime(2*INTERVAL + 10);
        test1 = store_.getTimeOfNextMessage(iter.getCurKey()) - clock_.getCurrentTime();
        assertTrue(test1 <= 0);
        iter.advanceTo(ackIdGen.getAckId(2*INTERVAL + 5));
        test1 = store_.getTimeOfNextMessage(iter.getCurKey()) - clock_.getCurrentTime();
        // System.out.println(store_.toString());
        assertEquals(10l, test1);
        test1 = store_.getTimeOfNextMessage(iter.getCurKey()) - clock_.getCurrentTime();
        assertEquals(10l, test1);
        assertTrue(iter.next() == null);
        test1 = store_.getTimeOfNextMessage(iter.getCurKey()) - clock_.getCurrentTime();
        assertEquals(10l,test1);
        clock_.setCurrentTime(2*INTERVAL + 20);
        test1 = store_.getTimeOfNextMessage(iter.getCurKey()) - clock_.getCurrentTime();
        assertTrue(test1 <= 0);
        assertTrue(iter.next().getAckId().getTime() == 2*INTERVAL + 20);
        test1 = store_.getTimeOfNextMessage(iter.getCurKey());
        assertEquals(Long.MAX_VALUE, test1);
    }

    protected AckIdV3 createAndEnqueueMsg(long dequeueTime) throws Exception {
        String payload = "message - test";
        Entry entry = new TestEntry(payload.getBytes(), dequeueTime, payload );
        try {
            AckIdV3 ackId = store_.getAckIdForEnqueue(dequeueTime);
            try {
                store_.enqueue(ackId, entry, true, null);
            } finally {
                store_.enqueueFinished(ackId);
            }
            return ackId;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("try to insert at " + dequeueTime);
            System.out.println("last available time " + store_.getLastAvailableId().getTime());
            // System.out.println(store_.toString());
            throw e;
        }
    }

    protected NavigableSet<AckIdV3> createAndEnqueueMsgs(long numMsg, long min, long max) throws Exception {
        TreeSet<AckIdV3> messages = new TreeSet<AckIdV3>();
        for (int i = 0; i < numMsg; i++) {
            long dequeueTime = min + random_.nextInt((int) (max - min));
            messages.add( createAndEnqueueMsg(dequeueTime) );
        }
        return messages;
    }

    protected Entry getStringEntry(final String s) {
        return new TestEntry(s.getBytes());
    }

}
