package com.amazon.messaging.utils;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.amazon.messaging.testing.TestExecutorService;


public class BoundedOfferTimeExecutorServiceTest {
    private static final class SetBooleanTask implements Runnable {
        private final AtomicBoolean bool;

        private SetBooleanTask(AtomicBoolean bool) {
            this.bool = bool;
        }

        @Override
        public void run() {
            bool.set(true);
        }
    }

    private static final class SleepTask implements Runnable {
        @Override
        public void run() {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // Just return
            }
        }
    }

    @Test
    public void testExecute() throws InterruptedException, ExecutionException, TimeoutException {
        BoundedOfferTimeExecutorService executor = new BoundedOfferTimeExecutorService( 
                1, 1, 1, TimeUnit.SECONDS, 1, 1, TimeUnit.SECONDS, true, "TestThread", true );

        final AtomicBoolean bool = new AtomicBoolean(false); 
        
        Future<?> future = 
                executor.submit( new SetBooleanTask(bool));
        
        future.get(1, TimeUnit.SECONDS);
        assertTrue( bool.get() );
        
        executor.shutdown();
    }
    
    @Test
    public void testTimeoutConstantWait() throws InterruptedException, ExecutionException, TimeoutException {
        final long maxOfferTime = 500;
        BoundedOfferTimeExecutorService executor = new BoundedOfferTimeExecutorService( 
                1, 1, 1, TimeUnit.SECONDS, 1, maxOfferTime, TimeUnit.MILLISECONDS, false, "TestThread", true );

        Future<?> future = 
                executor.submit( new SleepTask());
        
        final AtomicBoolean bool = new AtomicBoolean(false); 
        
        // This should sit in the buffer forever until the sleep task is cancelled
        Future<?> future2 = executor.submit( new SetBooleanTask(bool) );
        
        assertSubmitTimesout( executor, maxOfferTime );
        assertSubmitTimesout( executor, maxOfferTime );
        
        assertFalse( bool.get() );
        future.cancel(true);
        
        // Should run now that the first task is removed
        future2.get(1, TimeUnit.SECONDS);
        assertTrue( bool.get() );
        
        executor.shutdown();
    }
    
    @Test
    public void testTimeoutFastFailIfStuck() throws InterruptedException, ExecutionException, TimeoutException {
        final long maxOfferTime = 500;
        BoundedOfferTimeExecutorService executor = new BoundedOfferTimeExecutorService( 
                1, 1, 1, TimeUnit.SECONDS, 1, maxOfferTime, TimeUnit.MILLISECONDS, true, "TestThread", true );

        Future<?> future = 
                executor.submit( new SleepTask());
        
        final AtomicBoolean bool = new AtomicBoolean(false); 
        
        // This should sit in the buffer forever until the sleep task is cancelled
        Future<?> future2 = executor.submit( new SetBooleanTask(bool) );
        
        assertSubmitTimesout( executor, maxOfferTime );
        assertSubmitTimesout( executor, 0 );
        
        assertFalse( bool.get() );
        future.cancel(true);
        
        // Should run now that the first task is removed
        future2.get(1, TimeUnit.SECONDS);
        assertTrue( bool.get() );
        
        executor.shutdown();
    }
    
    @Test
    public void testParallelWait() throws InterruptedException, ExecutionException, TimeoutException {
        final long maxOfferTime = 500;
        final BoundedOfferTimeExecutorService executor = new BoundedOfferTimeExecutorService( 
                1, 1, 1, TimeUnit.SECONDS, 1, maxOfferTime, TimeUnit.MILLISECONDS, true, "TestThread", true );
        
        TestExecutorService testExecutorService = new TestExecutorService();

        Future<?> future = 
                executor.submit( new SleepTask());
        
        final AtomicBoolean bool = new AtomicBoolean(false); 
        
        // This should sit in the buffer forever until the sleep task is cancelled
        Future<?> future2 = executor.submit( new SetBooleanTask(bool) );
        
        testExecutorService.submit( 
                new Runnable() {
                    @Override
                    public void run() {
                        assertSubmitTimesout(executor, maxOfferTime);
                    }
                } );
        
        testExecutorService.submit( 
                new Runnable() {
                    @Override
                    public void run() {
                        assertSubmitTimesout(executor, maxOfferTime);
                    }
                } );
        
        testExecutorService.shutdownWithin(2, TimeUnit.SECONDS);
        testExecutorService.rethrow();
       
        assertFalse( bool.get() );
        future.cancel(true);
        
        // Should run now that the first task is removed
        future2.get(1, TimeUnit.SECONDS);
        assertTrue( bool.get() );
        
        executor.shutdown();
    }
    
    @Test
    public void testStaggeredParallelWait() throws InterruptedException, ExecutionException, TimeoutException {
        final long maxOfferTime = 500;
        final BoundedOfferTimeExecutorService executor = new BoundedOfferTimeExecutorService( 
                1, 1, 1, TimeUnit.SECONDS, 1, maxOfferTime, TimeUnit.MILLISECONDS, true, "TestThread", true );
        
        TestExecutorService testExecutorService = new TestExecutorService();

        Future<?> future = 
                executor.submit( new SleepTask());
        
        final AtomicBoolean bool = new AtomicBoolean(false); 
        
        // This should sit in the buffer forever until the sleep task is cancelled
        Future<?> future2 = executor.submit( new SetBooleanTask(bool) );
        
        testExecutorService.submit( 
                new Runnable() {
                    @Override
                    public void run() {
                        assertSubmitTimesout(executor, maxOfferTime);
                    }
                } );
        
        testExecutorService.submit( 
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        assertSubmitTimesout(executor, maxOfferTime - 100);
                    }
                } );
        
        testExecutorService.shutdownWithin(2, TimeUnit.SECONDS);
        testExecutorService.rethrow();
       
        assertFalse( bool.get() );
        future.cancel(true);
        
        // Should run now that the first task is removed
        future2.get(1, TimeUnit.SECONDS);
        assertTrue( bool.get() );
        
        executor.shutdown();
    }

    private void assertSubmitTimesout(
        final BoundedOfferTimeExecutorService executor, final long expectedTimeout)
    {
        long startTime = System.nanoTime();
        try {
            Future<?> future3 = executor.submit( new SleepTask() );
            future3.cancel( true );
            fail( "Should have timedout trying to add the task.");
        } catch( RejectedExecutionException e ) {
            long actualWaitTime = TimeUnit.NANOSECONDS.toMillis( System.nanoTime() - startTime );
            assertTrue( "Waited to long", actualWaitTime < expectedTimeout + 50  );
            if( expectedTimeout > 50 ) {
                assertTrue( "Returned to early", actualWaitTime >= expectedTimeout - 50 );
            }
        }
    }
}
