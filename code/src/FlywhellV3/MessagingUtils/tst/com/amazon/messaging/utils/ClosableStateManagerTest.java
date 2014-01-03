package com.amazon.messaging.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;

import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.utils.ClosableStateManager.State;

import static com.amazon.messaging.utils.ClosableStateManager.CloseableExceptionGenerator;


public class ClosableStateManagerTest extends TestCase {
    private static class StateManagerClosedException extends Exception {
        private static final long serialVersionUID = 1L;

        public StateManagerClosedException(String message) {
            super(message);
        }
        
    }
    
    @Test
    public void testSingleThread() throws StateManagerClosedException {
        ClosableStateManager<StateManagerClosedException> stateManager = 
            new ClosableStateManager<StateManagerClosedException>(
                new CloseableExceptionGenerator<StateManagerClosedException>() {
                    @Override
                    public StateManagerClosedException getException(String operation, State state) {
                        return new StateManagerClosedException( operation + ":" + state );
                    }
                }, false );
        
        stateManager.checkOpen("checkOpen");
        assertTrue( stateManager.isOpen() );
        
        stateManager.lockOpen("lockOpen");
        stateManager.unlock();
        
        assertTrue( stateManager.close() );
        assertFalse( stateManager.close() );
        
        assertFalse( stateManager.isOpen() );
        
        try {
            stateManager.checkOpen("checkOpenTest");
            fail( "checkOpen should have thrown.");
        } catch( StateManagerClosedException e ) {
            assertEquals( "checkOpenTest:" + State.CLOSED, e.getMessage() );
        }
        
        try {
            stateManager.lockOpen("lockOpenTest");
            fail( "lockOpen should have thrown.");
        } catch( StateManagerClosedException e ) {
            assertEquals( "lockOpenTest:" + State.CLOSED, e.getMessage() );
        }
    }
    
    @Test
    public void testNoNewThreadsAfterClose() throws InterruptedException {
        TestExecutorService executor = new TestExecutorService("testNoNewThreadsAfterClose");

        try {
            final ClosableStateManager<StateManagerClosedException> stateManager = 
                    new ClosableStateManager<StateManagerClosedException>(
                        new CloseableExceptionGenerator<StateManagerClosedException>() {
                            @Override
                            public StateManagerClosedException getException(String operation, State state) {
                                return new StateManagerClosedException( operation + ":" + state );
                            }
                        }, false );
            
            final CountDownLatch closeAboutToStartLatch = new CountDownLatch(1);
            final CountDownLatch newLockDone = new CountDownLatch(1);
            
            executor.submit( new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    stateManager.lockOpen("lockOpen");
                    try {
                        closeAboutToStartLatch.await();
                        Thread.sleep( 100 );
                        try {
                            stateManager.lockOpen("lockOpen");
                            stateManager.unlock();
                        } catch( StateManagerClosedException e) {
                            fail( "Threads that already have the lock should still be " +
                            		"able to get it even if close has started." );
                        }
                        
                        newLockDone.await();
                    } finally {
                        stateManager.unlock();
                    }
                    return null;
                }
            } );
            
            executor.submit( new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    closeAboutToStartLatch.countDown();
                    assertTrue( stateManager.close() );
                    return null;
                }
            } );
            
            executor.submit( new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    closeAboutToStartLatch.await();
                    Thread.sleep( 100 );
                    try {
                        stateManager.lockOpen("lockOpen");
                        stateManager.unlock();
                        fail( "It should not be possible to lock a ClosableStateManager once close has started.");
                    } catch (StateManagerClosedException e ) {
                        // Success
                    } finally {
                        newLockDone.countDown();
                    }
                    return null;
                }
            } );
            
            executor.shutdownWithin(1, TimeUnit.SECONDS);
            executor.rethrow();
        } finally {
            executor.shutdownNow();
        }
        
    }
}
