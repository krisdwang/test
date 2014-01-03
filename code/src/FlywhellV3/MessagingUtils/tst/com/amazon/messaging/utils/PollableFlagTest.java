package com.amazon.messaging.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazon.messaging.testing.TestExecutorService;


public class PollableFlagTest {
    @Test
    public void testNoWait() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("PollableFlagTest.testNoWait");
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    PollableFlag state = new PollableFlag(true);
                    assertEquals( true, state.get() );
                    assertTrue( state.waitForSet( Long.MAX_VALUE ) );
                    state.waitForSet();
                } catch( InterruptedException e ) {
                    throw new RuntimeException("Interrupted", e);
                }
            }
        });
        
        executorService.shutdownWithin( 100, TimeUnit.MILLISECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testTimeout() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("PollableFlagTest.testTimeout");
        
        final PollableFlag flag = new PollableFlag( false );
        final int numAttempts = 10;
        final CyclicBarrier barrier = new CyclicBarrier(2);
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    for( int i = 0; i < numAttempts; i++ ) {
                        long startTime = System.currentTimeMillis();
                        flag.waitForSet( 200 );
                        long waiting = System.currentTimeMillis() - startTime;
                        assertTrue( "Should wait for the timeout before giving up", waiting >= 200 );
                        assertTrue(
                                "Spurious wake ups should not extend the timeout beyond what was requested. Waited for " +
                                        waiting + " ms.", waiting <= 250);
                        barrier.await();
                    }
                } catch( InterruptedException e ) {
                    throw new RuntimeException("Interrupted", e);
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException("BrokenBarrierException", e);
                }
            }
        });
        
        // Test that another thread setting the same value over and over doesn't mess up the wait
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    for( int i = 0; i < numAttempts; i++ ) {
                        for( int j = 0; j < 12; j++ ) {
                            flag.clear();
                            Thread.sleep( 1 );
                        }
                        barrier.await();
                    }
                } catch( InterruptedException e ) {
                    throw new RuntimeException("Interrupted", e);
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException("BrokenBarrierException", e);
                }
            }
        });
        
        executorService.shutdownWithin( 4, TimeUnit.SECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testSet() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("PollableFlagTest.testSet");
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    PollableFlag state = new PollableFlag(false);
                    state.set( true );
                    assertTrue( state.waitForSet( Long.MAX_VALUE ) );
                    state.waitForSet();
                    assertEquals( true, state.get() );
                } catch( InterruptedException e ) {
                    throw new RuntimeException("Interrupted", e);
                }
            }
        });
        
        executorService.shutdownWithin( 100, TimeUnit.MILLISECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testSetBetweenThreads() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("PollableFlagTest.testSetBetweenThreads");
        
        final PollableFlag state = new PollableFlag();
        final int numChanges = 100;
        final CyclicBarrier barrier = new CyclicBarrier(2);
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                boolean current = false;
                for( int i = 0; i < numChanges; i++ ) {
                    current = !current;
                    state.set( current );
                    try {
                        barrier.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException( "Interrupted");
                    } catch (BrokenBarrierException e) {
                        throw new RuntimeException( "Broker barrier");
                    }
                    if( i % 3 == 0 ) Thread.yield();
                }
            }
        });
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                boolean current = false;
                for( int i = 0; i < numChanges; i++ ) {
                    current = !current;
                    try {
                        if( current ) state.waitForSet();
                        else state.waitForClear();
                        barrier.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException( "Interrupted");
                    } catch (BrokenBarrierException e) {
                        throw new RuntimeException( "Broker barrier");
                    }
                    if( i % 3 == 1 ) Thread.yield();
                }
            }
        });
        
        executorService.shutdownWithin( 1, TimeUnit.SECONDS );
        executorService.rethrow();
    }
}
