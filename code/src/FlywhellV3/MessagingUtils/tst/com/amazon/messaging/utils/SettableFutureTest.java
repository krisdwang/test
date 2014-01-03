package com.amazon.messaging.utils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.utils.SettableFuture;

import static org.junit.Assert.*;

public class SettableFutureTest {
    private class RecordAfterDoneSettableFuture<T> extends SettableFuture<T> {
        private final AtomicInteger afterDoneCallCount = new AtomicInteger(0);
        
        public RecordAfterDoneSettableFuture() {
            super();
        }

        @Override
        protected void afterDone() {
            super.afterDone();
            int result = afterDoneCallCount.incrementAndGet();
            assertEquals( "afterDone called multiple times", 1, result );
            assertTrue( isDone() || isCancelled() );
        }

        public boolean afterDoneCalled() {
            return afterDoneCallCount.get() > 0;
        }
    }
    
    @Test
    public void testSingleThreadSet() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    RecordAfterDoneSettableFuture<Integer> future = new RecordAfterDoneSettableFuture<Integer>();
                    assertFalse( future.afterDoneCalled() );
                    assertFalse( future.isDone() );
                    assertFalse( future.isCancelled() );
                    
                    future.set( 5 );
                    assertTrue( future.afterDoneCalled() );
                    try {
                        assertEquals( 5, future.get().intValue() );
                    } catch (ExecutionException e) {
                        throw new RuntimeException( "Get of a non exception threw exception", e );
                    }
                    
                    assertTrue( future.isDone() );
                    assertFalse( future.isCancelled() );
                    
                    try {
                        future.set( 5 );
                        fail( "Set of already set future did not fail.");
                    } catch( IllegalStateException e ) {
                        // Do nothing
                    }
                    
                    try {
                        future.setException( new RuntimeException() );
                        fail( "Set of already set future did not fail.");
                    } catch( IllegalStateException e ) {
                        // Do nothing
                    }

                } catch( InterruptedException e ) {
                    throw new RuntimeException( "Interrupted", e );
                }
            }
        });
        
        executorService.shutdownWithin( 1, TimeUnit.SECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testSingleThreadSetException() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    RecordAfterDoneSettableFuture<Integer> future = new RecordAfterDoneSettableFuture<Integer>();
                    assertFalse( future.afterDoneCalled() );
                    assertFalse( future.isDone() );
                    assertFalse( future.isCancelled() );
                    
                    Exception exception = new RuntimeException( "Test" );
                    future.setException( exception );
                    assertTrue( future.afterDoneCalled() );
                    try {
                        future.get();
                        fail( "Exception was no thrown." );
                    } catch (ExecutionException e) {
                        assertSame( exception, e.getCause() );
                    }
                    
                    assertTrue( future.isDone() );
                    assertFalse( future.isCancelled() );
                    
                    try {
                        future.set( 5 );
                        fail( "Set of already set future did not fail.");
                    } catch( IllegalStateException e ) {
                        // Do nothing
                    }
                    try {
                        future.setException( new RuntimeException() );
                        fail( "Set of already set future did not fail.");
                    } catch( IllegalStateException e ) {
                        // Do nothing
                    }
                } catch( InterruptedException e ) {
                    throw new RuntimeException( "Interrupted", e );
                }
            }
        });
        
        executorService.shutdownWithin( 1, TimeUnit.SECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testSingleThreadCancel() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    RecordAfterDoneSettableFuture<Integer> future = new RecordAfterDoneSettableFuture<Integer>();
                    assertFalse( future.afterDoneCalled() );
                    assertFalse( future.isDone() );
                    assertFalse( future.isCancelled() );
                    
                    // Make sure the thread hasn't already been interrupted
                    if( Thread.interrupted() ) throw new InterruptedException();
                    
                    future.setRunningThread( Thread.currentThread() );
                    future.cancel(true);
                    assertTrue( future.afterDoneCalled() );
                    
                    assertTrue( Thread.interrupted() );
                    
                    try {
                        future.get();
                        fail( "Exception was no thrown." );
                    } catch (ExecutionException e) {
                        throw new RuntimeException( "Get of a cancelled future threw exception", e );
                    } catch( CancellationException e ) {
                        // Do nothing
                    }
                    
                    assertTrue( future.isDone() );
                    assertTrue( future.isCancelled() );

                    // Should both be ignored
                    future.set( 5 );
                    future.setException( new RuntimeException() );
                } catch( InterruptedException e ) {
                    throw new RuntimeException( "Interrupted", e );
                }
            }
        });
        
        executorService.shutdownWithin( 1, TimeUnit.SECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testTimeout() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    RecordAfterDoneSettableFuture<Integer> future = new RecordAfterDoneSettableFuture<Integer>();
                    assertFalse( future.afterDoneCalled() );
                    assertFalse( future.isDone() );
                    assertFalse( future.isCancelled() );
                    
                    long startTime = System.currentTimeMillis(); 
                    try {
                        future.get( 200, TimeUnit.MILLISECONDS );
                        fail( "Get of an unset future didn't timeout");
                    } catch (ExecutionException e) {
                        throw new RuntimeException( "Get of an unset future threw exception", e );
                    } catch (TimeoutException e) {
                        assertTrue( System.currentTimeMillis() - startTime >= 200 );
                    }
                    
                    assertFalse( future.afterDoneCalled() );
                    assertFalse( future.isDone() );
                    assertFalse( future.isCancelled() );

                    // Should be okay
                    future.set( 5 );
                    try {
                        assertEquals( 5, future.get().intValue() );
                    } catch (ExecutionException e) {
                        throw new RuntimeException( "Get of a non exception threw exception", e );
                    }
                    assertTrue( future.afterDoneCalled() );
                } catch( InterruptedException e ) {
                    throw new RuntimeException( "Interrupted", e );
                }
            }
        });
        
        executorService.shutdownWithin( 1, TimeUnit.SECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testSetBetweenThreads() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        final RecordAfterDoneSettableFuture<Integer> future = new RecordAfterDoneSettableFuture<Integer>();
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                assertFalse( future.isDone() );
                assertFalse( future.isCancelled() );
                assertFalse( future.afterDoneCalled() );
                
                future.set( 5 );
            }
        });
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals( 5, future.get().intValue() );
                    assertTrue( future.afterDoneCalled() );
                } catch (ExecutionException e) {
                    throw new RuntimeException( "Get of a non exception threw exception", e );
                } catch (InterruptedException e) {
                    throw new RuntimeException( "Interrupted", e );
                }
            }
        });
        
        executorService.shutdownWithin( 1, TimeUnit.SECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testSetExceptionBetweenThreads() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        final RecordAfterDoneSettableFuture<Integer> future = new RecordAfterDoneSettableFuture<Integer>();
        final RuntimeException exception = new RuntimeException();
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                assertFalse( future.isDone() );
                assertFalse( future.isCancelled() );
                assertFalse( future.afterDoneCalled() );
                
                future.setException( exception );
            }
        });
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    future.get();
                    fail( "Get did not throw an exception for an exception future.");
                } catch (ExecutionException e) {
                    assertSame( exception, e.getCause() );
                } catch (InterruptedException e) {
                    throw new RuntimeException( "Interrupted", e );
                }
                assertTrue( future.afterDoneCalled() );
            }
        });
        
        executorService.shutdownWithin( 1, TimeUnit.SECONDS );
        executorService.rethrow();
    }
}
