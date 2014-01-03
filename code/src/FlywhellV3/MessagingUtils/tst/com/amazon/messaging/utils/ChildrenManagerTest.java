package com.amazon.messaging.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.utils.ChildrenManager.ChildrenManagerClosedException;

import static org.junit.Assert.*;


public class ChildrenManagerTest {
    public static class StringChildManager 
        extends ChildrenManager<Integer, String, RuntimeException, RuntimeException> 
    {
        public StringChildManager() {
            super();
        }

        public StringChildManager(Map<Integer, String> initialValues) {
            super(initialValues);
        }

        @Override
        protected String createChild(Integer childId) throws RuntimeException {
            return childId.toString();
        }
        
        @Override
        public boolean remove(Integer childId, String child) throws ChildrenManagerClosedException {
            return super.remove(childId, child);
        }
        
        @Override
        public Map<Integer, String> closeManager() {
            return super.closeManager();
        }
    }
    
    @Test
    public void testGetAndCreate() throws ChildrenManagerClosedException {
        StringChildManager childManager = new StringChildManager();
        
        assertNull( childManager.get(1) );
        assertEquals( "1", childManager.getOrCreate( 1 ) );
        assertEquals( "1", childManager.get(1));
        assertNull( childManager.get(2) );
        
    }
        
    @Test
    public void testRemove() throws ChildrenManagerClosedException {
        StringChildManager childManager = new StringChildManager();
        
        assertFalse( childManager.remove(1, "1") );
        assertEquals( "1", childManager.getOrCreate(1));
        assertFalse( childManager.remove(1, "2") );
        assertTrue( "1", childManager.remove(1, "1") );
        assertNull( childManager.get(1) );
        assertFalse( childManager.remove(1, "1") );
        
        // Check that a removed item can be recreated
        assertEquals( "1", childManager.getOrCreate( 1 ) );
        assertEquals( "1", childManager.get(1));
    }
    
    @Test
    public void testKeysAndValues() throws ChildrenManagerClosedException {
        StringChildManager childManager = new StringChildManager();
        
        Set<Integer> keys = new HashSet<Integer>();
        assertEquals( keys, childManager.keySet() );
        assertEquals( 0, childManager.values().size() );
        assertTrue( childManager.values().isEmpty() );
        
        assertEquals( "1", childManager.getOrCreate(1));
        assertEquals( "2", childManager.getOrCreate(2));
        
        keys.addAll( Arrays.asList( 1, 2 ) );
        assertEquals( keys, childManager.keySet() );
        
        assertEquals( 2, childManager.values().size() );
        assertFalse( childManager.values().isEmpty() );
        assertTrue( childManager.values().contains( "1" ) );
        assertTrue( childManager.values().contains( "2" ) );
        int itrCount = 0;
        for( String val : childManager.values() ) {
            assertTrue( val.equals("1") || val.equals("2") );
            itrCount++;
        }
        assertEquals( 2, itrCount );
    }
    
    @Test
    public void testInitialValueMap() throws ChildrenManagerClosedException {
        Map<Integer,String> baseMap = new HashMap<Integer, String>();
        baseMap.put( 1, "1" );
        baseMap.put( 2, "2" );
        
        StringChildManager childManager = new StringChildManager(baseMap);
        assertEquals( "1", childManager.get(1));
        assertEquals( "2", childManager.get(2));
    }
    
    @Test
    public void testClose() throws ChildrenManagerClosedException {
        StringChildManager childManager = new StringChildManager();
        assertEquals( "1", childManager.getOrCreate(1));
        assertEquals( "2", childManager.getOrCreate(2));
        
        Map<Integer,String> expected = new HashMap<Integer, String>();
        expected.put( 1, "1" );
        expected.put( 2, "2" );
        
        assertEquals( expected, childManager.closeManager() );
        assertEquals( Collections.emptyMap(), childManager.closeManager() );
        
        assertEquals( Collections.emptySet(), childManager.keySet() );
        assertTrue( childManager.values().isEmpty() );
        
        try {
            childManager.getOrCreate(1);
            fail( "Operation succeeded on closed childManager" );
        } catch( ChildrenManagerClosedException e ) {
        }
        
        try {
            childManager.get(1);
            fail( "Operation succeeded on closed childManager" );
        } catch( ChildrenManagerClosedException e ) {
        }
        
        try {
            childManager.remove(1, "1");
            fail( "Operation succeeded on closed childManager" );
        } catch( ChildrenManagerClosedException e ) {
        }
    }
    
    @Test
    public void testHandlesCreateFail() throws ChildrenManagerClosedException {
        final int failId = 101;
        final String failMessage = "Create Failed";
        
        StringChildManager failManager = new StringChildManager() {
            @Override
            protected String createChild(Integer childId) throws RuntimeException {
                if( childId.equals( failId ) ) {
                    throw new RuntimeException(failMessage);
                }
                return super.createChild(childId);
            };
        };
        
        assertEquals( "1", failManager.getOrCreate(1));
        try {
            failManager.getOrCreate(failId);
        } catch( RuntimeException e ) {
            assertEquals( e.getMessage(), failMessage );
        }
        
        assertNull( failManager.get( failId ) );
        assertEquals( Collections.singleton( 1 ), failManager.keySet() );
        assertEquals( 1, failManager.values().size() );
    }
    
    @Test
    public void testGetCreateInProgress() throws InterruptedException {
        TestExecutorService executor = new TestExecutorService();
        
        final CountDownLatch startingGet = new CountDownLatch(1);
        final CountDownLatch inCreate = new CountDownLatch(1);
        
        final StringChildManager manager = new StringChildManager() {
            @Override
            protected String createChild(Integer childId) throws RuntimeException {
                try {
                    inCreate.countDown();
                    startingGet.await();
                    Thread.sleep(100);
                    return super.createChild(childId);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
        };
        
        executor.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    inCreate.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                startingGet.countDown();
                try {
                    assertEquals( "5", manager.get(5) );
                } catch (ChildrenManagerClosedException e) {
                    throw new RuntimeException(e);
                }
            }
        } );
        
        executor.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals( "5", manager.getOrCreate(5) );
                } catch (ChildrenManagerClosedException e) {
                    throw new RuntimeException(e);
                }
            }
        } );
        
        executor.shutdownWithin(5, TimeUnit.SECONDS);
        executor.rethrow();
    }
    
    @Test
    public void testCleanupCreateInProgress() throws InterruptedException {
        TestExecutorService executor = new TestExecutorService();
        
        final CountDownLatch startingClose = new CountDownLatch(1);
        final CountDownLatch inCreate = new CountDownLatch(1);
        
        final StringChildManager manager = new StringChildManager() {
            @Override
            protected String createChild(Integer childId) throws RuntimeException {
                try {
                    inCreate.countDown();
                    startingClose.await();
                    Thread.sleep(100);
                    return super.createChild(childId);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
        };
        
        executor.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    inCreate.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                startingClose.countDown();
                assertEquals( Collections.singletonMap(5, "5"), manager.closeManager() );
            }
        } );
        
        executor.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals( "5", manager.getOrCreate(5) );
                } catch (ChildrenManagerClosedException e) {
                    throw new RuntimeException(e);
                }
            }
        } );
        
        executor.shutdownWithin(5, TimeUnit.SECONDS);
        executor.rethrow();
    }
    
    @Test
    public void testFirstCreateFailsInParallel() throws InterruptedException {
        TestExecutorService executor = new TestExecutorService();
        
        final String createFailedMessage = "Create Failed";
        final CountDownLatch startingSecondCreate = new CountDownLatch(1);
        final CountDownLatch inFirstCreate = new CountDownLatch(1);
        final AtomicInteger createCount = new AtomicInteger(0);
        
        final StringChildManager manager = new StringChildManager() {
            @Override
            protected String createChild(Integer childId) throws RuntimeException {
                try {
                    if( createCount.getAndIncrement() == 0 ) {
                        inFirstCreate.countDown();
                        startingSecondCreate.await();
                        Thread.sleep(100);
                        throw new RuntimeException(createFailedMessage);
                    } else {
                        return super.createChild(childId);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
        };
        
        executor.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        manager.getOrCreate(5);
                        fail( "First create did not fail.");
                    } catch( RuntimeException e ) {
                        assertEquals( createFailedMessage, e.getMessage() );
                    }
                } catch (ChildrenManagerClosedException e) {
                    throw new RuntimeException(e);
                }
            }
        } );
        
        executor.execute( new Runnable() {
            @Override
            public void run() {
                try {
                    inFirstCreate.await();
                    startingSecondCreate.countDown();
                    assertEquals( "5", manager.getOrCreate(5) );
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ChildrenManagerClosedException e) {
                    throw new RuntimeException(e);
                }
            }
        } );
        
        
        
        executor.shutdownWithin(5, TimeUnit.SECONDS);
        executor.rethrow();
    }
    
    @Test
    public void testParallelCreate() throws InterruptedException, ChildrenManagerClosedException {
        final int numToCreate = 500;
        final AtomicBoolean abort = new AtomicBoolean(false);
        TestExecutorService executor = new TestExecutorService();
        
        final CyclicBarrier barrier = new CyclicBarrier(2);
        
        final StringChildManager manager = new StringChildManager() {
            @Override
            protected String createChild(Integer childId) throws RuntimeException {
                Thread.yield(); // Give a better chance of competition for the create 
                return super.createChild(childId);
            };
        };
        
        Runnable createTask = new Runnable() {
            @Override
            public void run() {
                for( int i = 0; i< numToCreate && !abort.get(); ++i ) {
                    try {
                        barrier.await();
                        assertEquals( Integer.toString(i), manager.getOrCreate(i) );
                    } catch (InterruptedException e) {
                        barrier.reset();
                        abort.set( true );
                        throw new RuntimeException(e);
                    } catch (BrokenBarrierException e) {
                        abort.set( true );
                        throw new RuntimeException(e);
                    } catch (ChildrenManagerClosedException e) {
                        abort.set( true );
                        barrier.reset();
                        throw new RuntimeException(e);
                    }
                    
                }
            }
        };
        
        executor.execute( createTask );
        executor.execute( createTask );
        
        executor.shutdownWithin(5, TimeUnit.SECONDS);
        executor.rethrow();
        
        assertEquals( numToCreate, manager.keySet().size() );
        for( int i = 0; i < numToCreate; ++i ) {
            assertEquals( String.valueOf(i), manager.get(i));
        }
    }
    
    @Test
    public void testParallelRemove() throws InterruptedException, ChildrenManagerClosedException {
        final int numToCreate = 500;
        final AtomicBoolean abort = new AtomicBoolean(false);
        TestExecutorService executor = new TestExecutorService();
        
        final CyclicBarrier barrier = new CyclicBarrier(2);
        
        final StringChildManager manager = new StringChildManager() {
            @Override
            protected String createChild(Integer childId) throws RuntimeException {
                Thread.yield(); // Give a better chance of competition for the create 
                return super.createChild(childId);
            };
        };
        
        for( int i = 0; i < numToCreate; ++i ) {
            assertEquals( String.valueOf(i), manager.getOrCreate(i));
        }
        
        final AtomicInteger removedCount = new AtomicInteger(0);
        
        Runnable removeTask = new Runnable() {
            @Override
            public void run() {
                for( int i = 0; i< numToCreate && !abort.get(); ++i ) {
                    try {
                        barrier.await();
                        if( manager.remove(i, String.valueOf(i)) ) {
                            removedCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        barrier.reset();
                        abort.set( true );
                        throw new RuntimeException(e);
                    } catch (BrokenBarrierException e) {
                        abort.set( true );
                        throw new RuntimeException(e);
                    } catch (ChildrenManagerClosedException e) {
                        abort.set( true );
                        barrier.reset();
                        throw new RuntimeException(e);
                    }
                    
                }
            }
        };
        
        executor.execute( removeTask );
        executor.execute( removeTask );
        
        executor.shutdownWithin(5, TimeUnit.SECONDS);
        executor.rethrow();
        
        assertEquals( Collections.emptySet(), manager.keySet() );
        assertEquals( numToCreate, removedCount.get() );
    }
}
