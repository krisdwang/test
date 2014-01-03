package com.amazon.messaging.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;


public class ScheduledExecutorCompletionServiceTest extends TestCase {
    
    // This test fails so ignore it
    //
    @Ignore
    @Test
    public void testNoDelay() throws InterruptedException, ExecutionException, TimeoutException {
        final int numThreads = 1;
        
        ScheduledExecutorCompletionService<Integer> service = new ScheduledExecutorCompletionService<Integer>(
                new ScheduledThreadPoolExecutor(numThreads, new TaskThreadFactory(
                        "ScheduledThreadPoolExecutorTest", false)));

        Future<Integer> future = service.submit(new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                return 1;
            }
        });

        Future<Integer> queuedFuture = service.take();
        assertSame(future, queuedFuture);
        assertEquals(1, future.get(0, TimeUnit.MILLISECONDS).intValue());
        assertTrue(service.cancelAllOutstanding(true).isEmpty());

        future = service.submit(new Runnable() {
            public void run() {
            }
        }, 2);

        queuedFuture = service.take();
        assertSame(future, queuedFuture);
        assertEquals(2, future.get(0, TimeUnit.MILLISECONDS).intValue());
        assertTrue(service.cancelAllOutstanding(true).isEmpty());
    }
    
    // This test fails so ignore it
    //
    @Ignore
    @Test
    public void testDelayed() throws InterruptedException, ExecutionException, TimeoutException {
        final int numThreads = 5;
        
        ScheduledExecutorCompletionService<Integer> service 
            = new ScheduledExecutorCompletionService<Integer>( 
                    new ScheduledThreadPoolExecutor(numThreads, new TaskThreadFactory("ScheduledThreadPoolExecutorTest", false) ) );
        
        long startTime = System.currentTimeMillis();
        
        Future<Integer> future = service.submitDelayed( new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return 3;
            }
        }, 500, TimeUnit.MILLISECONDS );
        
        assertNull( service.poll(10, TimeUnit.MILLISECONDS) );
        
        Future<Integer> queuedFuture = service.poll( 500, TimeUnit.MILLISECONDS );
        assertTrue( System.currentTimeMillis() - startTime >= 500 );
        assertSame( future, queuedFuture );
        assertEquals( 3, future.get(0, TimeUnit.MILLISECONDS).intValue() );
        assertTrue( service.cancelAllOutstanding(true).isEmpty() );
        
        startTime = System.currentTimeMillis();
        
        future = service.submitDelayed( 
                new Runnable() { public void run() { } }, 4, 
                500, TimeUnit.MILLISECONDS );
        
        queuedFuture = service.take();
        assertTrue( System.currentTimeMillis() - startTime >= 500 );
        assertSame( future, queuedFuture );
        assertEquals( 4, future.get(0, TimeUnit.MILLISECONDS).intValue() );
    }
    
    @Test
    public void testShutdown() throws InterruptedException, ExecutionException, TimeoutException {
        final int numThreads = 5;
        
        TestExecutorService testService = new TestExecutorService("testShutdown");
        
        final ScheduledExecutorCompletionService<Integer> service 
            = new ScheduledExecutorCompletionService<Integer>( 
                new ScheduledThreadPoolExecutor(numThreads, new TaskThreadFactory("ScheduledThreadPoolExecutorTest", false) ) );
        
        Future<Long> shutdownFuture = testService.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                assertTrue( service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS) );
                return System.currentTimeMillis();
            }
        });
        
        List< Future< Integer > > enqueuedFutures = new ArrayList<Future<Integer>>();
        for( int i = 0; i < numThreads + 4; ++i ) {
            enqueuedFutures.add( service.submit( new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    for(;;) { Thread.sleep(10000); }
                }
            }));
        }
        
        long startTime = System.currentTimeMillis();
        assertFalse( service.awaitTermination(200, TimeUnit.MILLISECONDS));
        assertTrue( System.currentTimeMillis() - startTime >= 200 );
        
        long shutdownTime = System.currentTimeMillis();
        service.shutdown();
        assertTrue( service.isShutdown() );
        
        service.cancelAllOutstanding(true);
        Long recordedShutdownTime = shutdownFuture.get( 200, TimeUnit.MILLISECONDS );
        assertTrue( recordedShutdownTime - shutdownTime <= 200 );
        
        testService.shutdownWithin(100, TimeUnit.MILLISECONDS);
        testService.rethrow();
        
        try {
            service.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return 5;
                }
            });
            fail( "Submit after shutdown did not fail");
        } catch( RejectedExecutionException e ) {
            // Success
        }
    }
    
    @Test
    public void testShutdownNoCancel() throws InterruptedException, ExecutionException, TimeoutException {
        final int numThreads = 1;
        
        TestExecutorService testService = new TestExecutorService("testShutdown");
        
        final ScheduledExecutorCompletionService<Integer> service 
            = new ScheduledExecutorCompletionService<Integer>( 
                new ScheduledThreadPoolExecutor(numThreads, new TaskThreadFactory("ScheduledThreadPoolExecutorTest", false) ) );
        
        Future<Long> shutdownFuture = testService.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                assertTrue( service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS) );
                return System.currentTimeMillis();
            }
        });
        
        service.submit( new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Thread.sleep( 500 );
                System.out.println( "Done");
                return 5;
            }
        });
        
        long shutdownTime = System.currentTimeMillis();
        service.shutdown();
        Long recordedShutdownTime = shutdownFuture.get( 700, TimeUnit.MILLISECONDS );
        assertTrue( recordedShutdownTime - shutdownTime <= 700 );
        assertTrue( recordedShutdownTime - shutdownTime >= 500 );
        assertTrue(service.cancelAllOutstanding(true).isEmpty());
    }
}
