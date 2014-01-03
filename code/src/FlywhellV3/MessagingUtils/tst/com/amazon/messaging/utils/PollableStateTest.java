package com.amazon.messaging.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;


public class PollableStateTest extends TestCase {
    private enum TestEnum {
        VALUE1, VALUE2;
    }
    
    @Test
    public void testNoWait() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    PollableState<TestEnum> state = new PollableState<TestEnum>(TestEnum.VALUE1);
                    assertEquals( TestEnum.VALUE1, state.get() );
                    assertTrue( state.waitForState( TestEnum.VALUE1, Long.MAX_VALUE ) );
                    state.waitForState(TestEnum.VALUE1);
                    assertTrue( state.waitForOtherState( TestEnum.VALUE2, Long.MAX_VALUE ) );
                    state.waitForOtherState(TestEnum.VALUE2);
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
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        final PollableState<TestEnum> state = new PollableState<TestEnum>(TestEnum.VALUE1);
        final int numAttempts = 10;
        final CyclicBarrier barrier = new CyclicBarrier(2);
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    for( int i = 0; i < numAttempts; i++ ) {
                        long startTime = System.currentTimeMillis();
                        assertFalse( state.waitForState( TestEnum.VALUE2, 100 ) );
                        assertFalse( state.waitForOtherState( TestEnum.VALUE1, 100 ) );
                        long waiting = System.currentTimeMillis() - startTime;
                        assertTrue( "Should wait for the timeout before giving up", waiting >= 200 );
                        assertTrue( "Spurious wake ups should not extend the timeout beyond what was requested", waiting <= 240 );
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
                        for( int j = 0; j < 10; j++ ) {
                            state.set( TestEnum.VALUE1 );
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
        
        executorService.shutdownWithin( 3, TimeUnit.SECONDS );
        executorService.rethrow();
    }
    
    @Test
    public void testSet() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    PollableState<TestEnum> state = new PollableState<TestEnum>(TestEnum.VALUE2);
                    state.set( TestEnum.VALUE1 );
                    assertTrue( state.waitForState( TestEnum.VALUE1, Long.MAX_VALUE ) );
                    assertTrue( state.waitForOtherState( TestEnum.VALUE2, Long.MAX_VALUE ) );
                    assertEquals( TestEnum.VALUE1, state.get() );
                } catch( InterruptedException e ) {
                    throw new RuntimeException("Interrupted", e);
                }
            }
        });
        
        executorService.shutdownWithin( 100, TimeUnit.MILLISECONDS );
        executorService.rethrow();
    }
    
    private static TestEnum swapValue( TestEnum value ) {
        if( value == TestEnum.VALUE1 ) return TestEnum.VALUE2;
        return TestEnum.VALUE1;
    }
    
    @Test
    public void testSetBetweenThreads() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService("SettableFutureTest");
        
        final TestEnum initialValue = TestEnum.VALUE2;
        final PollableState<TestEnum> state = new PollableState<TestEnum>(initialValue);
        final int numChanges = 100;
        final CyclicBarrier barrier = new CyclicBarrier(3);
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                TestEnum newValue = initialValue;
                for( int i = 0; i < numChanges; i++ ) {
                    newValue = swapValue( newValue );
                    state.set( newValue );
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
                TestEnum waitForValue = initialValue;
                for( int i = 0; i < numChanges; i++ ) {
                    try {
                        waitForValue = swapValue( waitForValue );
                        state.waitForState(waitForValue);
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
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                TestEnum waitForValue = initialValue;
                for( int i = 0; i < numChanges; i++ ) {
                    try {
                        state.waitForOtherState(waitForValue);
                        waitForValue = swapValue( waitForValue );
                        barrier.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException( "Interrupted");
                    } catch (BrokenBarrierException e) {
                        throw new RuntimeException( "Broker barrier");
                    }
                    if( i % 3 == 2 ) Thread.yield();
                }
            }
        });
        
        executorService.shutdownWithin( 1, TimeUnit.SECONDS );
        executorService.rethrow();
    }
}
