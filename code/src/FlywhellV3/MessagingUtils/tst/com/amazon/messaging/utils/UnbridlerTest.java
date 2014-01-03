package com.amazon.messaging.utils;


import static org.junit.Assert.*;

import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Data;

import org.junit.Test;

import com.amazon.messaging.concurent.Unbridler;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;


public class UnbridlerTest extends TestCase {
    @Test
    public void testAlwaysAvailable() throws InterruptedException {
        Unbridler< String, String, RuntimeException> unbridler = new Unbridler<String, String, RuntimeException>() {
            @Override
            protected boolean acquireAvailable() throws RuntimeException {
                return true;
            }
            
            @Override
            protected String execute(String arg) throws RuntimeException {
                assertEquals( "Arg", arg );
                return "Success";
            }
        };
        
        long startTime = System.currentTimeMillis();
        assertEquals( "Success", unbridler.call(1000, "Arg") );
        assertTrue( "Unbridler shouldn't block if there is data available", System.currentTimeMillis() - startTime <= 100 );
    }
    
    @Test
    public void testNeverAvailable() throws InterruptedException {
        Unbridler< String, String, RuntimeException> unbridler = new Unbridler<String, String, RuntimeException>() {
            @Override
            protected boolean acquireAvailable() throws RuntimeException {
                return false;
            }
            
            @Override
            protected String execute(String arg) throws RuntimeException {
                return "Failure";
            }
        };
        
        long startTime = System.currentTimeMillis();
        assertNull( "Unbridler should not return result if there is no data available", unbridler.call(500, "Arg") );
        long duration = System.currentTimeMillis() - startTime;
        assertTrue( "Unbridler should block for timeout", duration >= 500 );
        assertTrue( "Unbridler shouldn't block for more than the timeout", duration <= 600 );
    }
    
    @Test
    public void testNotAvailableTillNotified() throws InterruptedException {
        TestExecutorService executor = new TestExecutorService("UnbridlerTest");
        final AtomicBoolean available = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Unbridler< String, String, RuntimeException> unbridler = new Unbridler<String, String, RuntimeException>() {
            @Override
            protected boolean acquireAvailable() throws RuntimeException {
                return available.get();
            }
            
            @Override
            protected String execute(String arg) throws RuntimeException {
                assertEquals( "Arg", arg );
                return "Success";
            }
        };
        
        executor.submit( new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    Thread.sleep( 100 );
                    available.set(true);
                    unbridler.wakeUpWaitingThread();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        
        executor.submit( new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    long startTime = System.currentTimeMillis();
                    assertEquals( "Success", unbridler.call(1000, "Arg") );
                    long duration = System.currentTimeMillis() - startTime;
                    // Not 100 as this thread may not have woken up until a little after the other test thread woke up
                    assertTrue( "Unbridler shouldn't return early", duration >= 50 );
                    assertTrue( "Unbridler shouldn't take too long after notification", duration <= 200 );
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                
            }
        });
        
        executor.shutdownWithin(1500, TimeUnit.MILLISECONDS);
        executor.rethrow();
    }
    
    @Test
    public void testStuckExecuteDoesNotBlockNotification() throws InterruptedException, ExecutionException {
        TestExecutorService executor = new TestExecutorService("UnbridlerTest");
        final CyclicBarrier barrier = new CyclicBarrier(2);
        
        final Unbridler< String, String, RuntimeException> unbridler = new Unbridler<String, String, RuntimeException>() {
            @Override
            protected boolean acquireAvailable() throws RuntimeException {
                return true;
            }
            
            @Override
            protected String execute(String arg) throws RuntimeException {
                try {
                    barrier.await();
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // Do nothing
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                return "Success";
            }
        };
        
        Future<?> callExecuteFuture = executor.submit( new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals( "Success", unbridler.call(1000, "Arg") );
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        
        Future<?> callWakeUpFuture = executor.submit( new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await(); // Wait till the other thread is inside execute
                    long startTime = System.currentTimeMillis();
                    unbridler.wakeUpWaitingThread();
                    assertTrue( "WakeUpWaitingThread should not block", System.currentTimeMillis() - startTime < 100 );
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        
        try {
            callWakeUpFuture.get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            fail( "WakeUpWaitingThread should not block");
        }
        callExecuteFuture.cancel( true );
        
        executor.shutdownWithin(500, TimeUnit.MILLISECONDS);
        executor.rethrow();
    }
    
    @Test
    public void testOverrideGetWaitTime() throws InterruptedException, ExecutionException {
        final long timeMessageIsAvailable = System.currentTimeMillis() + 500;
        
        final Unbridler< String, String, RuntimeException> unbridler = new Unbridler<String, String, RuntimeException>() {
            @Override
            protected long getWaitTime(long currentTime, long endTime) throws RuntimeException {
                return Math.max( 0, timeMessageIsAvailable - System.currentTimeMillis());
            }
            
            @Override
            protected boolean acquireAvailable() throws RuntimeException {
                return System.currentTimeMillis() >= timeMessageIsAvailable;
            }
            
            @Override
            protected String execute(String arg) throws RuntimeException {
                assertTrue( "Execute called early", System.currentTimeMillis() >= timeMessageIsAvailable );
                return "Success";
            }
        };
        
        assertEquals( "Success", unbridler.call(1000, "Arg") );
        assertTrue( "Call returned early", System.currentTimeMillis() >= timeMessageIsAvailable );
    }
    
    @Test
    public void testChangingGetWaitTime() throws InterruptedException {
        TestExecutorService executor = new TestExecutorService("UnbridlerTest");
        final long startTime = System.currentTimeMillis();
        final AtomicLong availableTime = new AtomicLong(startTime + 1000);
        
        final Semaphore alreadyWaiting = new Semaphore(0);
        
        final Unbridler< String, String, RuntimeException> unbridler = new Unbridler<String, String, RuntimeException>() {
            @Override
            protected long getWaitTime(long currentTime, long endTime) throws RuntimeException {
                long retval = Math.max( 0, availableTime.get() - System.currentTimeMillis());
                alreadyWaiting.release();
                return retval;
            }
            
            @Override
            protected boolean acquireAvailable() throws RuntimeException {
                return System.currentTimeMillis() >= availableTime.get();
            }
            
            @Override
            protected String execute(String arg) throws RuntimeException {
                assertTrue( "Execute called early", System.currentTimeMillis() >= availableTime.get() );
                return "Success";
            }
        };
        
        executor.submit( new Runnable() {
            @Override
            public void run() {
                try {
                    alreadyWaiting.acquire();
                    availableTime.set(startTime + 500);
                    unbridler.wakeUpWaitingThread();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        
        assertEquals( "Success", unbridler.call(1000, "Arg") );
        assertTrue( "Call returned early", System.currentTimeMillis() >= availableTime.get() );
        assertTrue( "Woke up late", System.currentTimeMillis() <= availableTime.get() + 100 );
        assertEquals( "Update never happened", startTime + 500, availableTime.get() );
        
        executor.shutdownWithin(100, TimeUnit.MILLISECONDS);
        executor.rethrow();
    }
    
    @Data
    private static class DelayMesssage implements Comparable<DelayMesssage> {
        private static final AtomicInteger sequence = new AtomicInteger(0);
        
        private final int id;
        private final long availableTime;
        
        
        public DelayMesssage(long availableTime) {
            this.id = sequence.incrementAndGet();
            this.availableTime = availableTime;
        }
        
        @Override
        public int compareTo(DelayMesssage o) {
            DelayMesssage om = ( DelayMesssage ) o;
            if( availableTime < om.availableTime ) {
                return -1;
            } else if( availableTime > om.availableTime ) {
                return 1;
            } else return id - om.id;
        }
    }
    
    @Test
    public void testMultiThreaded() throws InterruptedException {
        final TestExecutorService executor = new TestExecutorService("UnbridlerTest");
        final Random random = new Random();
        final AtomicInteger threadsInExectute = new AtomicInteger(0);
        final AtomicComparableReference<Integer> maxThreadsInExecute = new AtomicComparableReference<Integer>(0);
        final PriorityQueue<DelayMesssage> messages = new PriorityQueue<DelayMesssage>();
        final AtomicInteger executeCounter = new AtomicInteger(0);
        final AtomicInteger nullResultCounter = new AtomicInteger(0);
        final AtomicInteger dequeueCounter = new AtomicInteger(0);
        final int messagesToEnqueuePerThread = 50;
        final int dequeueThreads = 15;
        final int enqueueThreads = 3;
        final int maxDelay = 100;
        final int totalMessagesEnqueued = messagesToEnqueuePerThread * enqueueThreads;
        
        final Unbridler< Integer, String, RuntimeException> unbridler = new Unbridler<Integer, String, RuntimeException>() {
            @Override
            protected long getWaitTime(long currentTime, long endTime) throws RuntimeException {
                synchronized (messages) {
                    DelayMesssage next = messages.peek();
                    if( next == null ) return Math.max( 0, endTime - currentTime );
                    
                    long realEndTime = Math.max( endTime, next.getAvailableTime() );
                    return Math.max( 0, realEndTime - currentTime );
                }
            }
            
            @Override
            protected boolean acquireAvailable() throws RuntimeException {
                synchronized (messages) {
                    DelayMesssage next = messages.peek();
                    if( next == null ) return false;
                    
                    return next.getAvailableTime() <= System.currentTimeMillis();
                }
            }
            
            @Override
            protected Integer execute(String arg) throws RuntimeException {
                maxThreadsInExecute.increaseTo( threadsInExectute.incrementAndGet() );
                executeCounter.incrementAndGet();
                Thread.yield();
                try {
                    synchronized (messages) {
                        DelayMesssage next = messages.poll();
                        assertNotNull(next);
                        return next.getId();
                    }
                } finally {
                    threadsInExectute.decrementAndGet();
                }
            }
        };
        
        for( int i = 0; i < dequeueThreads; ++i ) {
            executor.execute( new Runnable() {
                @Override
                public void run() {
                    while( dequeueCounter.get() < totalMessagesEnqueued ) {
                        Integer result;
                        try {
                            result = unbridler.call(1000, "Arg");
                        } catch (InterruptedException e) {
                            if( dequeueCounter.get() < totalMessagesEnqueued ) { 
                                throw new RuntimeException("Interrupted before all messages were received." );
                            }
                            // Silently swallow an interrupt at the end
                            return;
                        }
                        
                        if( result != null ) {
                            dequeueCounter.incrementAndGet();
                        } else {
                            nullResultCounter.incrementAndGet();
                        }
                    }
                }
            } );
        }
        
        for( int i = 0; i < enqueueThreads; ++i ) {
            executor.execute( new Runnable() {
                @Override
                public void run() {
                    for( int j = 0; j <messagesToEnqueuePerThread; ++j ) {
                        synchronized (messages) {
                            messages.add( new DelayMesssage( System.currentTimeMillis() + random.nextInt( maxDelay ) ) );
                        }
                        unbridler.wakeUpWaitingThread();
                        // So that enqueues don't all finish before dequeues
                        try {
                            Thread.sleep( 10 );
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
        }
        
        executor.shutdownWithin( 5000, TimeUnit.MILLISECONDS );
        executor.rethrow();
        
        assertEquals( 1, maxThreadsInExecute.get().intValue() );
        assertEquals( totalMessagesEnqueued, dequeueCounter.get() );
    }
}
