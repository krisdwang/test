package com.amazon.messaging.seqstore.v3.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.InflightEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightInfoFactory;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;

/**
 * Tests for InflightImpl w/ custom InfoType and update method.
 * 
 * @author saville
 */
public class InflightImplTest extends TestCase {
    private static final Log log = LogFactory.getLog(InflightImplTest.class);
	
	// keep <= 2^15...
    private static final int TEST_MESSAGE_COUNT = (int) Math.pow(2, 12); // power of two please.
	
	private static final AckIdGenerator ACK_ID_GEN = new AckIdGenerator();
	
	private static final int DEFAULT_TIMEOUT = 10;
	
	private static final BasicInflightInfoFactory DEFAULT_INFO_FACTORY = BasicInflightInfoFactory.DEFAULT_INSTANCE;
	
    /**
     * {@link InflightInfoFactory} wrapper which allows tests to run code
     * in another thread synchronized with when the dequeue method invokes
     * {@link SynchronizingInflightInfoFactory#getMessageInfoForDequeue(Object)}.
     * 
     * NOTE: this class is only meant to be used for synchronizing between
     *       exactly one dequeue thread and exactly one other thread.
     */
    private static class SynchronizingInflightInfoFactory<InfoType> implements InflightInfoFactory<InfoType> {
        private final InflightInfoFactory<InfoType> inflightInfoFactory;
        
        private final CyclicBarrier barrier;
        private final Semaphore semaphore;
        
        public SynchronizingInflightInfoFactory(
                InflightInfoFactory<InfoType> inflightInfoFactory) {
            this.inflightInfoFactory = inflightInfoFactory;
            this.barrier = new CyclicBarrier(2);
            this.semaphore = new Semaphore(0);
        }
        
        /**
         * Call this method to wait until {@link #getMessageInfoForDequeue(Object)}
         * is invoked during a dequeue, after dequeue has fetched a
         * re-deliverable message. This must be called exactly once for each
         * dequeue operation that uses this factory.
         */
        public void waitForInvocation() {
            try {
                barrier.await();
            } catch (InterruptedException e) {
                throw new RuntimeException("interrupted at barrier!", e);
            } catch (BrokenBarrierException e) {
                throw new RuntimeException("barrier broken!", e);
            }
        }
        
        /**
         * Call this method to allow {@link #getMessageInfoForDequeue(Object)} to
         * complete, allowing dequeue to update the dequeued message. This must
         * be called exactly once for each dequeue operation that uses this
         * factory.
         */
        public void permitCompletion() {
            semaphore.release();
        }

        @Override
        public InfoType getMessageInfoForDequeue(InfoType currentInfo) {
            waitForInvocation();
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException("interrupted at semaphore!");
            }
            return inflightInfoFactory.getMessageInfoForDequeue(currentInfo);
        }

        @Override
        public int getTimeoutForDequeue(InfoType messageInfo) {
            return inflightInfoFactory.getTimeoutForDequeue(messageInfo);
        }
        
    }
    
    private final boolean isCoveragePass;
    
    
    public InflightImplTest() {
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
    }

    public void testEmpty() {
        // Test actions on readers that have never had an add, ack or ack level update
        SettableClock clock = new SettableClock();
        InflightImpl<BasicInflightInfo> inflight = 
                new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
        AckIdV3 fakeAckId = ACK_ID_GEN.getAckId( 1 );
        
        assertEquals( new AckIdV3(0, false), inflight.getAckLevel() );
        assertNull( inflight.peekNextRedeliverable() );
        assertEquals(Long.MAX_VALUE, inflight.getNextRedeliveryTime() );
        assertNull( inflight.dequeueMessage( BasicInflightInfoFactory.DEFAULT_INSTANCE ) );
        assertNull( inflight.getInflightEntryInfo( fakeAckId ) );
        assertEquals( InflightMetrics.ZeroInFlight, inflight.getInflightMetrics() );
        inflight.makeAllDeliverable();
        assertEquals( Collections.emptyList(), inflight.getAllMessagesInFlight() );
        
        assertEquals( InflightUpdateResult.NOT_FOUND, update( inflight, fakeAckId, 1000,
        		new BasicInflightInfo(2), new BasicInflightInfo(3)) );
        
        inflight.ack( fakeAckId);
        assertEquals( new AckIdV3( fakeAckId, true ), inflight.getAckLevel() );
    }
        
    @Test
    public void testTimeout() {
        final int timeout = 10;
        SettableClock clock = new SettableClock();
        InflightImpl<BasicInflightInfo> inflight = 
                new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
        
        assertEquals(Long.MAX_VALUE, inflight.getNextRedeliveryTime() );

        // . . . . . .
        // 1 2 3 4 5 6 time created
        // 11 12 13 14 15 16 will time out at
        // 12 step clock to here and assert nacked/inflight

        AckIdV3 ackId[] = new AckIdV3[9];

        for (int i = 1; i <= 6; i++) {
            clock.setCurrentTime(i);

            ackId[i] = ACK_ID_GEN.getAckId(System.currentTimeMillis());
            addMessage(inflight, ackId[i], timeout );
        }
        
        assertEquals( new InflightMetrics(6,0), inflight.getInflightMetrics() );

        clock.setCurrentTime(10);
        assertEquals(0, inflight.getInflightMetrics().getNumAvailableForRedelivery());
        assertEquals(11, inflight.getNextRedeliveryTime() );
        clock.setCurrentTime(12);
        assertEquals( new InflightMetrics(4,2), inflight.getInflightMetrics() );
        assertEquals(11, inflight.getNextRedeliveryTime() );

        // add some at the current time - these are already timed out.
        for (int i = 7; i <= 8; i++) {
            ackId[i] = ACK_ID_GEN.getAckId(System.nanoTime());
            addMessage(inflight, ackId[i], 0 );
        }
        
        assertEquals( new InflightMetrics(4,4), inflight.getInflightMetrics() );

        // Reschedule message 2 from time 12 to time 14
        assertEquals( InflightUpdateResult.DONE, update( inflight, ackId[2], 2, null, null) );
        
        assertEquals( new InflightMetrics(5,3), inflight.getInflightMetrics() );
        clock.setCurrentTime(14);
        assertEquals( new InflightMetrics(2,6), inflight.getInflightMetrics() );
        
        // Extend timeout of message 1 to 16
        assertEquals( InflightUpdateResult.DONE, update( inflight, ackId[1], 4, null, null) );
        assertEquals( new InflightMetrics(3,5), inflight.getInflightMetrics() );
        
        inflight.ack( ackId[1] );
        assertEquals(12, inflight.getNextRedeliveryTime() );
        
        inflight.makeAllDeliverable();
        assertEquals( new InflightMetrics(0,7), inflight.getInflightMetrics() );
        
        // Ack everything
        for (int i = 2; i <= 8; i++) {
            inflight.ack( ackId[i] );
        }
        
        assertEquals(Long.MAX_VALUE, inflight.getNextRedeliveryTime() );
        assertEquals( InflightMetrics.ZeroInFlight, inflight.getInflightMetrics() );
    }
    
    @Test
    public void testAddingToInflightAckingThenGettingAckLevel() {
        final int timeout = 60 * 1000;
        SettableClock clock = new SettableClock();
        InflightImpl<BasicInflightInfo> inflight = 
                new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
        AckIdV3 ackId[] = new AckIdV3[TEST_MESSAGE_COUNT];

        // create all ackIds.
        for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
            ackId[i] = ACK_ID_GEN.getAckId(System.currentTimeMillis());
        }

        // sent to inflight
        for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
            addMessage(inflight, ackId[i], timeout);
        }
        // ack level should be before the first message id.
        assertEquals(new AckIdV3(ackId[0], false), inflight.getAckLevel());
        final int halfCount = TEST_MESSAGE_COUNT / 2;

        for (int i = 0; i < halfCount; i++) {
            inflight.ack( ackId[i]);
        }

        // ack level should be the halfCount message id.
        assertEquals(new AckIdV3(ackId[halfCount], false), inflight.getAckLevel());

        // test the getting the ackLevel using a progressive acking sequence.
        int start = halfCount;
        int increment = 2;
        int ackLevel = halfCount + increment - 1;
        while( ackLevel < TEST_MESSAGE_COUNT ) {
            for (int i = start; i < TEST_MESSAGE_COUNT; i += increment) {
                inflight.ack( ackId[i]);
            }
            assertEquals(new AckIdV3(ackId[ackLevel], false), inflight.getAckLevel());
            start = ackLevel;
            increment = increment * 2;
            ackLevel = halfCount + increment - 1;
        }
        assertEquals(new AckIdV3(ackId[TEST_MESSAGE_COUNT - 1], false),
                inflight.getAckLevel());
        
        // Ack the final message
        inflight.ack( ackId[TEST_MESSAGE_COUNT - 1]);
        assertEquals(new AckIdV3(ackId[TEST_MESSAGE_COUNT - 1], true), 
                inflight.getAckLevel());
        
        assertEquals( InflightMetrics.ZeroInFlight, inflight.getInflightMetrics() );
    }
    
    @Test
    public void testDoubleAdd() {
        SettableClock clock = new SettableClock();
        InflightImpl<BasicInflightInfo> inflight = 
                new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
        AckIdV3 message = new AckIdV3(1, false);
        
        addMessage(inflight, message, DEFAULT_TIMEOUT);
        InflightEntryInfo<AckIdV3, BasicInflightInfo> messageInfo =
        	inflight.getInflightEntryInfo( message);
        assertEquals(message, messageInfo.getAckId());
        assertEquals(1, messageInfo.getInflightInfo().getDeliveryCount());
        assertEquals(DEFAULT_TIMEOUT, messageInfo.getDelayUntilNextRedrive());
        
        clock.setCurrentTime( 5 );
        try {
            addMessage(inflight, message, DEFAULT_TIMEOUT);
            fail( "Second add did not fail" );
        } catch( IllegalStateException e ) {
            // Success
        }
        
        InflightEntryInfo<AckIdV3, BasicInflightInfo> messageInfo2 =
        	inflight.getInflightEntryInfo( message);
        assertEquals(messageInfo.getInflightInfo(), messageInfo2.getInflightInfo());
        assertEquals(6, messageInfo2.getDelayUntilNextRedrive());
    }
    
    @Test
    public void testSetAckLevel() {
        SettableClock clock = new SettableClock();
        InflightImpl<BasicInflightInfo> inflight = 
                new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
        clock.setCurrentTime(1);
        assertNull(dequeueMessage(inflight));

        AckIdV3 m1 = new AckIdV3(1, false);
        AckIdV3 m2 = new AckIdV3(5, false);
        AckIdV3 m3 = new AckIdV3(3, false);
        AckIdV3 m4 = new AckIdV3(4, false);
        AckIdV3 m5 = new AckIdV3(2, false);
        AckIdV3 m6 = new AckIdV3(6, false);
        
        addMessage(inflight, m1, DEFAULT_TIMEOUT);
        addMessage(inflight, m2, DEFAULT_TIMEOUT);
        addMessage(inflight, m3, DEFAULT_TIMEOUT);
        addMessage(inflight, m4, DEFAULT_TIMEOUT);
        addMessage(inflight, m5, DEFAULT_TIMEOUT);
        addMessage(inflight, m6, DEFAULT_TIMEOUT);
        
        nackMessage(inflight, m1);
        nackMessage(inflight, m2);
        nackMessage(inflight, m3);
        nackMessage(inflight, m4);
        nackMessage(inflight, m5);
        nackMessage(inflight, m6);
        
        AckIdV3 d1 = dequeueMessage(inflight);
        AckIdV3 d2 = dequeueMessage(inflight);
        AckIdV3 d3 = dequeueMessage(inflight);
        AckIdV3 d4 = dequeueMessage(inflight);
        AckIdV3 d5 = dequeueMessage(inflight);
        AckIdV3 d6 = dequeueMessage(inflight);
        assertNull(dequeueMessage(inflight));
        
        assertEquals(d1, m1);
        assertEquals(d2, m5);
        assertEquals(d3, m3);
        assertEquals(d4, m4);
        assertEquals(d5, m2);
        assertEquals(d6, m6);
        
        nackMessage(inflight, d1);
        nackMessage(inflight, d2);
        nackMessage(inflight, d3);
        nackMessage(inflight, d4);
        nackMessage(inflight, d5);
        nackMessage(inflight, d6);
        
        d1 = dequeueMessage(inflight);
        d2 = dequeueMessage(inflight);
        d3 = dequeueMessage(inflight);

        inflight.setAckLevel( new AckIdV3(3,true));
        nackMessage(inflight, d1);
        nackMessage(inflight, d2);
        nackMessage(inflight, d3);
        
        AckIdV3 f1 = dequeueMessage(inflight);
        AckIdV3 f2 = dequeueMessage(inflight);
        AckIdV3 f3 = dequeueMessage(inflight);
        assertNull(dequeueMessage(inflight));
        
        assertEquals(f1, m4);
        assertEquals(f2, m2);
        assertEquals(f3, m6);
    }
    
    @Test
    public void testDequeueWithConcurrentChanges() throws Exception {
        final SettableClock clock = new SettableClock();
        final InflightImpl<BasicInflightInfo> inflight = 
                new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
        final SynchronizingInflightInfoFactory<BasicInflightInfo> infoFactory =
            new SynchronizingInflightInfoFactory<BasicInflightInfo>(
                    new BasicInflightInfoFactory(10));
        clock.setCurrentTime(1);
        
        final AckIdV3 m1 = new AckIdV3(1, false);
        final AckIdV3 m2 = new AckIdV3(2, false);
        
        addMessage(inflight, m1, 0);
        clock.setCurrentTime(2);
        addMessage(inflight, m2, 0);
        
        // test updating m1 w/ new info
        // m1 won't dequeue, but m2 will
        Thread updateInfoThread = new Thread("Update Message #1 Info") {
            @Override
            public void run() {
                // concurrently modify m1 to prevent dequeue
                infoFactory.waitForInvocation();
                update(inflight, m1, 5, null, new BasicInflightInfo(4));
                infoFactory.permitCompletion();
                
                // allow m2 to be dequeued
                infoFactory.waitForInvocation();
                infoFactory.permitCompletion();
            }
        };
        updateInfoThread.start();
        AckIdV3 dq1 = dequeueMessage(inflight, infoFactory);
        assertNotNull(dq1);
        assertEquals(m2, dq1);
        assertEquals(5, info(inflight, m1).getDelayUntilNextRedrive());
        assertEquals(10, info(inflight, m2).getDelayUntilNextRedrive());
        updateInfoThread.join();
        
        // test updating m1 w/ new timeout
        // m1 should still be dequeued
        Thread updateTimeoutThread = new Thread("Update Message #1 Timeout") {
            @Override
            public void run() {
                // concurrently modify the timeout for m1
                infoFactory.waitForInvocation();
                update(inflight, m1, 15, null, null);
                infoFactory.permitCompletion();
            }
        };
        updateTimeoutThread.start();
        clock.setCurrentTime(12);
        AckIdV3 dq2 = dequeueMessage(inflight, infoFactory);
        assertNotNull(dq2);
        assertEquals(m1, dq2);
        assertEquals(10, info(inflight, m1).getDelayUntilNextRedrive());
        assertEquals(0, info(inflight, m2).getDelayUntilNextRedrive());
        updateTimeoutThread.join();
        
        // test updating m2 w/ new info, but same timeout
        // m2 should be dequeued
        Thread updateOnlyInfoThread = new Thread("Update Message #2 Info Only") {
            @Override
            public void run() {
                // concurrent change info, but not timeout for m2
                infoFactory.waitForInvocation();
                update(inflight, m2, -1, null, new BasicInflightInfo(5));
                infoFactory.permitCompletion();
                
                // allow m2 to be dequeued on second pass
                infoFactory.waitForInvocation();
                infoFactory.permitCompletion();
            }
        };
        updateOnlyInfoThread.start();
        AckIdV3 dq3 = dequeueMessage(inflight, infoFactory);
        assertNotNull(dq3);
        assertEquals(m2, dq3);
        assertEquals(10, info(inflight, m1).getDelayUntilNextRedrive());
        InflightEntryInfo<AckIdV3, BasicInflightInfo> dq3info = info(inflight, m2);
        assertEquals(10, dq3info.getDelayUntilNextRedrive());
        assertEquals(6, dq3info.getInflightInfo().getDeliveryCount());
        updateOnlyInfoThread.join();
        
        // check that there are no duplicated entries in the re-delivery map
        assertEquals(new InflightMetrics(2, 0), inflight.getInflightMetrics());
        assertEquals(22, inflight.getNextRedeliveryTime());
        ackMessage(inflight, m1);
        assertEquals(new InflightMetrics(1, 0), inflight.getInflightMetrics());
        assertEquals(22, inflight.getNextRedeliveryTime());
        ackMessage(inflight, m2);
        assertEquals(new InflightMetrics(0, 0), inflight.getInflightMetrics());
        assertEquals(Long.MAX_VALUE, inflight.getNextRedeliveryTime());
    }
    
    @Test
    public void testUpdate() {
        final SettableClock clock = new SettableClock();
        final InflightImpl<BasicInflightInfo> inflight = 
                new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
        clock.setCurrentTime(1);
        
        final AckIdV3 m1 = new AckIdV3(1, false);
        final AckIdV3 m2 = new AckIdV3(2, false);
        
        BasicInflightInfo info[] = new BasicInflightInfo[10];
        info[0] = BasicInflightInfo.firstDequeueInfo();
        for (int i = 1; i <= 5; i++) {
            info[i] = info[i-1].nextDequeueInfo();
        }

        // test once using m1
        
        // update before add
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m1, 10, null, null));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m1, 10, info[0], null));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m1, -1, null, info[1]));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m1, 10, info[1], info[2]));
        
        // add and update
        addMessage(inflight, m1, info[0], 10);
        assertEquals(InflightUpdateResult.DONE, update(inflight, m1, 10, null, null));
        assertEquals(InflightUpdateResult.DONE, update(inflight, m1, 10, info[0], null));
        assertEquals(InflightUpdateResult.DONE, update(inflight, m1, -1, null, info[1]));
        assertEquals(InflightUpdateResult.DONE, update(inflight, m1, 10, info[1], info[2]));
        
        // unexpected update
        assertEquals(InflightUpdateResult.EXPECTATION_UNMET, update(inflight, m1, 10, info[0], null));
        assertEquals(info[2], info(inflight, m1).getInflightInfo());
        assertEquals(InflightUpdateResult.EXPECTATION_UNMET, update(inflight, m1, 10, info[0], info[1]));
        assertEquals(info[2], info(inflight, m1).getInflightInfo());
        
        // still update-able after unexpected update
        assertEquals(InflightUpdateResult.DONE, update(inflight, m1, 10, info[2], info[3]));
        
        // test again using m2
        
        // update before add
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m2, 10, null, null));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m2, 10, info[0], null));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m2, -1, null, info[1]));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m2, 10, info[1], info[2]));
        
        // add and update
        addMessage(inflight, m2, info[0], 10);
        assertEquals(InflightUpdateResult.DONE, update(inflight, m2, 10, null, null));
        assertEquals(InflightUpdateResult.DONE, update(inflight, m2, 10, info[0], null));
        assertEquals(InflightUpdateResult.DONE, update(inflight, m2, -1, null, info[1]));
        assertEquals(InflightUpdateResult.DONE, update(inflight, m2, 10, info[1], info[2]));
        
        // unexpected update
        assertEquals(InflightUpdateResult.EXPECTATION_UNMET, update(inflight, m2, 10, info[0], null));
        assertEquals(info[2], info(inflight, m2).getInflightInfo());
        assertEquals(InflightUpdateResult.EXPECTATION_UNMET, update(inflight, m2, 10, info[0], info[1]));
        assertEquals(info[2], info(inflight, m2).getInflightInfo());
        
        // still update-able after unexpected update
        assertEquals(InflightUpdateResult.DONE, update(inflight, m2, 10, info[2], info[3]));
        
        // ack m1 and try to update
        assertTrue(ackMessage(inflight, m1));
        assertFalse(ackMessage(inflight, m1));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m1, 10, info[3], info[4]));
        assertEquals(InflightUpdateResult.DONE, update(inflight, m2, 10, info[3], info[4]));
        
        // ack m2 and try to update
        assertTrue(ackMessage(inflight, m2));
        assertFalse(ackMessage(inflight, m2));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m1, 10, info[3], info[4]));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m2, 10, info[3], info[4]));
        assertEquals(InflightUpdateResult.NOT_FOUND, update(inflight, m2, 10, info[4], info[5]));
    }
    
    private static <InfoType> void checkAckLevels(InflightImpl<InfoType> inflight, AtomicLong sequence, SortedSet< AckIdV3 > outstandingSet ) {
        // Lowest must be fetched before ack level as the ack level could increase because of an ack between the statements
        AckIdV3 lowestPossible;
        
        try {
            lowestPossible = new AckIdV3( outstandingSet.first(), false );
        } catch( NoSuchElementException e ){
            // Only happens if all message have been acked. Can't just check outstandingSet.isEmpty as that 
            //  may change between checks. Can't use the sequence as adds may have happened that haven't
            //  yet been added to outstandingSet.
            lowestPossible = null;
        }
        
        AckIdV3 ackLevel = inflight.getAckLevel();
        
        // Max possible must go after fetching the ack level as a new message could be enqueued between the statements
        AckIdV3 maxPossibleAckLevel = new AckIdV3(0, sequence.get(), 0, true, null );
        
        assertNotNull( maxPossibleAckLevel );
        assertTrue( 
                "AckLevel = " + ackLevel + " which is more than the maximum possible level of " + maxPossibleAckLevel,
                ackLevel.compareTo(maxPossibleAckLevel) <= 0);
        if( lowestPossible != null ) 
            assertTrue("ackLevel = " + ackLevel + ", lowestPossible = " + lowestPossible + ", max = " + maxPossibleAckLevel, 
                        ackLevel.compareTo(lowestPossible) >= 0);
    }

    @Test
    public void testThreadSafety() throws Throwable {
        final int timeout = 1;
        final int batchSize = 100;
        
        final Random rand = new Random();
        final SettableClock clock = new SettableClock();
        final InflightImpl<BasicInflightInfo> inflight = 
                new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
        
        final AtomicBoolean stopped = new AtomicBoolean(false);
        final AtomicLong sequence = new AtomicLong(0);
        Thread[] threads = new Thread[100];
        final Vector<Throwable> exceptions = new Vector<Throwable>();
        final Vector<AckIdV3> outstanding = new Vector<AckIdV3>();
        final SortedSet< AckIdV3 > outstandingSet = 
            Collections.synchronizedSortedSet( new TreeSet<AckIdV3>() );
        final Object lock = new Object();
        
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {

                @Override
                public void run() {
                    try {
                        long start = System.currentTimeMillis();
                        while (!stopped.get() && exceptions.isEmpty() ) {
                            int wait = rand.nextInt(4000);
                            Thread.sleep(wait);

                            for (int j = 0; j < batchSize; j++) {
                                AckIdV3 ackId;
                                synchronized (lock) {
                                    // Synchronized block to ensure that items are added to inflight in order
                                    // of increasing ack id. If that doesn't happen then the ack level doesn't 
                                    // behave as it should.
                                    ackId = new AckIdV3(0, sequence.incrementAndGet(), 0);
                                    addMessage(inflight, ackId, timeout);
                                    outstandingSet.add( ackId );
                                }
                                
                                outstanding.add(ackId);

                                checkAckLevels(inflight, sequence, outstandingSet);
                            }
                            for (int j = 0; j < batchSize; j++) {
                                nackMessage(inflight, outstanding.get(batchSize - (j + 1)));
                                checkAckLevels(inflight, sequence, outstandingSet);
                            }
                            for (int j = 0; j < batchSize; j++) {
                                AckIdV3 ackToRemove = outstanding.remove(batchSize - (j + 1));
                                inflight.ack( ackToRemove);
                                outstandingSet.remove( ackToRemove );
                                checkAckLevels(inflight, sequence, outstandingSet);
                            }
                            System.out.println("done after " + (System.currentTimeMillis() - start));
                        }
                    } catch (Throwable e) {
                        log.warn( "Error handling exception: ", e );
                        exceptions.add(e);
                    }
                }
            };
        }
        for (Thread thread : threads) {
            thread.start();
        }
        Thread.sleep(2 * 1000);
        stopped.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
        if (exceptions.size() > 0)
            throw exceptions.get(0);
        assertEquals(InflightMetrics.ZeroInFlight, inflight.getInflightMetrics() );

        AckIdV3 ackLevel = inflight.getAckLevel();
        assertNotNull( ackLevel );
        assertEquals(new AckIdV3(0, sequence.get(), 0, true, null), ackLevel);
        assertTrue(new AckIdV3(0, sequence.get(), 0, true, null).compareTo(ackLevel) >= 0);
        assertEquals( InflightMetrics.ZeroInFlight, inflight.getInflightMetrics() );
    }
    
    /**
     * fat tailed, not really exponential
     * 
     * @param numberOfUniqueStores
     * @return
     */
    private static List<InflightImpl<BasicInflightInfo>> exponentialDistributionOfStores(int numberOfUniqueStores, Clock clock) {
        List<InflightImpl<BasicInflightInfo>> buckets = new ArrayList<InflightImpl<BasicInflightInfo>>();

        for (int x = 0; x < numberOfUniqueStores; x++) {
            int px = (int) Math.floor(Math.pow((x + 50) * 1.01, -1.01 * x + 1));
            InflightImpl<BasicInflightInfo> s = new InflightImpl<BasicInflightInfo>(null, AckIdV3.MINIMUM, clock);
            for (int i = 0; i < px; i++) {
                buckets.add(s);
            }
        }
        
        return buckets;
    }
    
    /**
     * Speed of is measured on the cross-product of all methods
     * 
     * @throws Throwable 
     */
    @Test
    public void testBenchmarkingOfContentionOfAllMethods() throws Throwable {
        if( isCoveragePass ) return;
        
        final SettableClock clock = new SettableClock();
        
        final ArrayList<TestOp> ops = new ArrayList<TestOp>();
        // ops.add(new AddOp());
        ops.add(new RemoveOp());
        ops.add(new GetAckLevelOp());
        ops.add(new PeekNextNackedAckIdOp());
        ops.add(new DequeueNextNackedAckIdOp());
        ops.add(new GetInFlighMessageInfoOp());
        ops.add(new NackOp());
        ops.add(new GetNumInFlightOp());
        ops.add(new GetNumNackedOp());

        Thread[] threads = new Thread[13];
        for (final TestOp op1 : ops) {
            for (final TestOp op2 : ops) {
                final Queue<Long> times = new ConcurrentLinkedQueue<Long>();
                final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<Throwable>();
                {
                    for (int i = 0; i < threads.length; i++) {
                        final int num = i;
                        threads[i] = new Thread() {

                            @Override
                            public void run() {
                                try {
                                    times.add((new TwoOpRunnable(clock, op1, op2, num)).call());
                                } catch (Throwable e) {
                                    exceptions.add(e);
                                }
                            }
                        };
                    }
                }
                for (Thread thread : threads) {
                    thread.start();
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                if (!exceptions.isEmpty()) 
                    throw exceptions.element();
                long sum = 0;
                for (int i = 0; i < threads.length; i++) {
                    Long time = times.poll();
                    assertNotNull(time);
                    sum += time;
                }
                System.out.println((sum / threads.length) + "\t avg millis \t"
                        + "for (add, dequeueNextNackedAckId, " + op1 + ", " + op2 + ")");
            }
        }
        System.out.print(" ");
    }

    private abstract class TestOp {
        public abstract Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id);
    }

    private class RemoveOp extends TestOp {
        @Override
        public String toString() {
            return "remove";
        }

        @Override
        public Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id) {
            inflight.ack(id);
            return null;
        }
    }

    private class GetAckLevelOp extends TestOp {
        @Override
        public String toString() {
            return "getAckLevel";
        }

        @Override
        public Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id) {
            return inflight.getAckLevel();
        }
    }

    private class PeekNextNackedAckIdOp extends TestOp {
        @Override
        public String toString() {
            return "peekNextNackedAckId";
        }

        @Override
        public Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id) {
            return inflight.peekNextRedeliverable();
        }
    }

    private class DequeueNextNackedAckIdOp extends TestOp {
        @Override
        public String toString() {
            return "dequeueNextNackedAckId";
        }

        @Override
        public Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id) {
            return dequeueMessage(inflight);
        }
    }

    private class GetInFlighMessageInfoOp extends TestOp {
        @Override
        public String toString() {
            return "getInflightMessageInfo";
        }

        @Override
        public Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id) {
            return inflight.getInflightEntryInfo(id);
        }
    }

    private class NackOp extends TestOp {
        @Override
        public String toString() {
            return "nack";
        }

        @Override
        public Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id) {
            return nackMessage(inflight, id);
        }
    }

    private class GetNumInFlightOp extends TestOp {
        @Override
        public String toString() {
            return "getNumInFlight";
        }

        @Override
        public Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id) {
            return inflight.getInflightMetrics().getNumInFlight();
        }
    }

    private class GetNumNackedOp extends TestOp {
        @Override
        public String toString() {
            return "getNumNacked";
        }

        @Override
        public Object op(InflightImpl<BasicInflightInfo> inflight, AckIdV3 id) {
            return inflight.getInflightMetrics().getNumAvailableForRedelivery();
        }
    }

    private class TwoOpRunnable implements Callable<Long> {
        private final TestOp op1;

        private final TestOp op2;

        static final int numStores = 200;

        static final int numRuns = 10;

        final List<InflightImpl<BasicInflightInfo>> buckets;

        final AtomicLong sequence = new AtomicLong(0);

        private final int theadNum;

        public TwoOpRunnable(Clock clock, TestOp op1, TestOp op2, int threadNum) {
            this.op1 = op1;
            this.op2 = op2;
            theadNum = threadNum;
            
            buckets = exponentialDistributionOfStores(numStores, clock);
        }

        @Override
        public Long call() {
            final Random rand = new Random(theadNum);
            final InflightImpl<BasicInflightInfo> inflight = buckets.get(rand.nextInt(buckets.size()));
            for (int adds = 0; adds < 1000; adds++) {
                addMessage( inflight, new AckIdV3(0, sequence.incrementAndGet(), 0), 10000);
            }
            final long start = System.currentTimeMillis();

            for (int i = 0; i < numRuns; i++) {
                rand.setSeed(i + rand.nextInt());
                Thread.yield();
                switch (rand.nextInt(3) + 1) {
                case 1:
                    for (int adds = 0; adds < 100; adds++) {
                        op1.op(inflight, new AckIdV3(0, sequence.incrementAndGet(), 0));
                    }
                    break;
                case 2:
                    for (int adds = 0; adds < 100; adds++) {
                        op2.op(inflight, new AckIdV3(0, sequence.incrementAndGet(), 0));
                    }
                    break;
                case 3:
                    // watch for memory usage with add!
                    addMessage( inflight, new AckIdV3(0, sequence.incrementAndGet(), 0), 10000);
                    dequeueMessage( inflight );
                    break;
                default: throw new IllegalStateException();
                }
            }
            long time = System.currentTimeMillis() - start;
            return time;
        }

    }
    
    private static void addMessage(InflightImpl<BasicInflightInfo> inflight, AckIdV3 ackId, int timeout) {
        addMessage(inflight, ackId, BasicInflightInfo.firstDequeueInfo(), timeout);
    }
    
    private static <InfoType> void addMessage(InflightImpl<InfoType> inflight, AckIdV3 ackId, InfoType inflightInfo, int timeout) {
        inflight.add(ackId, inflightInfo, timeout);
    }
    
    private static <InfoType> AckIdV3 dequeueMessage(InflightImpl<InfoType> inflight, InflightInfoFactory<InfoType> infoFactory) {
    	InflightEntryInfo<AckIdV3, InfoType> dequeuedEntryInfo = inflight.dequeueMessage(infoFactory);
    	if (dequeuedEntryInfo == null) return null;
    	
    	assertNotNull(dequeuedEntryInfo.getAckId());
    	assertNotNull(dequeuedEntryInfo.getInflightInfo());
    	
    	int expectedTimeout = infoFactory.getTimeoutForDequeue(dequeuedEntryInfo.getInflightInfo());
    	assertEquals(expectedTimeout, dequeuedEntryInfo.getDelayUntilNextRedrive());
    	
    	return dequeuedEntryInfo.getAckId();
    }
    
    private static AckIdV3 dequeueMessage(InflightImpl<BasicInflightInfo> inflight) {
        return dequeueMessage(inflight, DEFAULT_INFO_FACTORY);
    }
    
    private static <InfoType> boolean ackMessage(InflightImpl<InfoType> inflight, AckIdV3 ackId) {
        return inflight.ack( ackId);
    }
    
    private static <InfoType> boolean nackMessage(InflightImpl<InfoType> inflight, AckIdV3 ackId) {
    	InflightUpdateResult nackResult = inflight.update(
    	        ackId, new InflightUpdateRequest<InfoType>().withNewTimeout(0) ); 
    	
    	switch (nackResult) {
    	case DONE:
    		return true;
    	case NOT_FOUND:
    		return false;
    	case EXPECTATION_UNMET:
    	default:
    	    fail("Unexpected result of NACK operation!");
    	    throw new RuntimeException();
    	}
    }
    
    private static <InfoType> InflightUpdateResult update(InflightImpl<InfoType> inflight, AckIdV3 ackId, int timeout, InfoType expect, InfoType update) {
        InflightUpdateRequest<InfoType> request = new InflightUpdateRequest<InfoType>();
        if( timeout > 0 ) {
            request.withNewTimeout(timeout);
        }
        if( expect != null ) {
            request.withExpectedInflightInfo(expect);
        }
        if( update != null ) {
            request.withNewInflightInfo(update);
        }
        
        return inflight.update( ackId, request );
    }
    
    private static <InfoType> InflightEntryInfo<AckIdV3, InfoType> info(InflightImpl<InfoType> inflight, AckIdV3 ackId) {
        return inflight.getInflightEntryInfo( ackId);
    }
}
