package com.amazon.messaging.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazon.messaging.concurent.LockingMap;
import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.utils.Pair;


public class LockingMapTest {
    @Test
    public void testBasicMapFunctions() {
        LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        
        assertTrue( map.isEmpty() );
        assertFalse( map.containsKey( "5" ) );
        assertFalse( map.containsValue( 5 ) );
        assertNull( map.get( "5" ) );
        assertTrue( map.entrySet().isEmpty() );
        assertTrue( map.values().isEmpty() );
        assertTrue( map.keySet().isEmpty() );
        
        assertNull( map.lock("5") );
        assertNull( map.remove( "5" ) );
        map.unlock("5");
        
        assertTrue( map.isEmpty() );
        
        Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
        assertFalse( itr.hasNext() );
        
        // Put "5" -> 5 in the map
        assertNull( map.lock("5") );
        assertNull( map.put("5", 5) );
        map.unlock("5");
        
        assertFalse( map.isEmpty() );
        assertTrue( map.containsKey( "5" ) );
        assertTrue( map.containsValue( 5 ) );
        assertEquals( 5, map.get( "5" ).intValue() );
        assertFalse( map.entrySet().isEmpty() );
        assertFalse( map.values().isEmpty() );
        assertFalse( map.keySet().isEmpty() );
        assertEquals( Collections.singleton("5"), map.keySet() );
        
        itr = map.entrySet().iterator();
        assertTrue( itr.hasNext() );
        assertEquals( new Pair<String, Integer>( "5", 5 ), itr.next() );
        assertFalse( itr.hasNext() );
        
        // remove the entry
        assertEquals( 5, map.lock("5").intValue() );
        map.unlockWithNewValue("5", null);
        
        
        // The map should be back to empty
        assertTrue( map.isEmpty() );
        assertFalse( map.containsKey( "5" ) );
        assertFalse( map.containsValue( 5 ) );
        assertNull( map.get( "5" ) );
        assertTrue( map.entrySet().isEmpty() );
        assertTrue( map.values().isEmpty() );
        assertTrue( map.keySet().isEmpty() );
    }
    
    @Test
    public void testModifyNotLocked() {
        LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        map.lock("5");
        map.unlockWithNewValue("5", 5);
        
        try {
            map.put("1", 1);
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            map.put("5", 5);
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            map.remove("1");
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            map.remove("5");
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
            assertTrue( itr.hasNext() );
            assertEquals( new Pair<String, Integer>("5", 5), itr.next() );
            itr.remove();
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
    }
    
    @Test
    public void testModifyLockedByOtherThread() throws InterruptedException {
        final LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        map.lock("5");
        map.unlockWithNewValue("5", 5);
        
        TestExecutorService executorService = new TestExecutorService();
        
        final CountDownLatch gotLockLatch = new CountDownLatch(1);
        final CountDownLatch doneTestLatch = new CountDownLatch(1);
        
        executorService.execute( new Runnable() {
            @Override
            public void run() {
                map.lock("5");
                gotLockLatch.countDown();
                try {
                    doneTestLatch.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                map.unlock("5");
            }
        });
        
        gotLockLatch.await(1, TimeUnit.SECONDS);
        
        try {
            map.put("1", 1);
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            map.put("5", 5);
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            map.remove("1");
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            map.remove("5");
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
            assertTrue( itr.hasNext() );
            assertEquals( new Pair<String, Integer>("5", 5), itr.next() );
            itr.remove();
            fail( "Unlocked write allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        doneTestLatch.countDown();
        executorService.shutdownWithin(1, TimeUnit.SECONDS);
        executorService.rethrow();
    }
    
    @Test
    public void testUnlockNotLocked() throws InterruptedException {
        LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        try {
            map.unlock("5");
            fail( "Unlocked of not locked allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        assertTrue( map.isEmpty() );
        
        try {
            map.lock("5");
            map.unlockWithNewValue("5", 1);
            map.unlock("5");
            fail( "Unlocked of not locked allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
        
        try {
            map.lock("5");
            map.unlock("5");
            map.unlockWithNewValue("5", 1);
            fail( "Unlocked of not locked allowed");
        } catch( IllegalMonitorStateException e ) {
            // Success
        }
    }
    
    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testGetLockedWithExistingValue() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService();
        final LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        final CountDownLatch keyLockedLatch = new CountDownLatch(1);
        final String key = "Key";
        map.lock(key);
        map.unlockWithNewValue(key, 1);
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                map.lock(key);
                keyLockedLatch.countDown();
                pause(20);
                map.put(key, 2);
                map.unlock(key);
            }
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertEquals( 2, map.get(key).intValue() );
            } 
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
                assertTrue( itr.hasNext() );
                assertEquals( new Pair<String, Integer>(key, 2), itr.next() );
                assertFalse( itr.hasNext() );
            } 
        });
       
        
        executorService.shutdownWithin(1, TimeUnit.SECONDS);
        executorService.rethrow();
    }
    
    @Test
    public void testGetLockedWithNewValue() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService();
        final LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        final CountDownLatch keyLockedLatch = new CountDownLatch(1);
        final String key = "Key";
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                map.lock(key);
                keyLockedLatch.countDown();
                pause(20);
                map.unlockWithNewValue(key, 2);
            }
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertEquals( 2, map.get(key).intValue() );
            } 
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
                assertTrue( itr.hasNext() );
                assertEquals( new Pair<String, Integer>(key, 2), itr.next() );
                assertFalse( itr.hasNext() );
            } 
        });
        
        executorService.shutdownWithin(1, TimeUnit.SECONDS);
        executorService.rethrow();
    }
    
    @Test
    public void testGetLockedWithRemovedValue() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService();
        final LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        final CountDownLatch keyLockedLatch = new CountDownLatch(1);
        final String key = "Key";
        
        map.lock(key);
        map.unlockWithNewValue(key, 1);
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                map.lock(key);
                keyLockedLatch.countDown();
                pause(20);
                map.unlockWithNewValue(key, null);
            }
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertNull( map.get(key) );
            } 
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertFalse( map.containsValue(1) );
            } 
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
                assertFalse( itr.hasNext() );
            } 
        });
        
        executorService.shutdownWithin(1, TimeUnit.SECONDS);
        executorService.rethrow();
    }
    
    @Test
    public void testRemoveUsingIterator() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService();
        final LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        final CountDownLatch keyLockedLatch = new CountDownLatch(1);
        final String key = "Key";
        
        map.lock(key);
        map.unlockWithNewValue(key, 1);
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                map.lock(key);
                keyLockedLatch.countDown();
                pause(20);
                Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
                assertTrue( itr.hasNext() );
                assertEquals( new Pair<String, Integer>( key, 1 ), itr.next() );
                itr.remove();
                assertFalse( itr.hasNext() );
                map.unlock(key);
            }
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertNull( map.get(key) );
            } 
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertFalse( map.containsValue(1) );
            } 
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
                assertFalse( itr.hasNext() );
            } 
        });
        
        executorService.shutdownWithin(1, TimeUnit.SECONDS);
        executorService.rethrow();
    }
    
    @Test
    public void testGetLockedNeverAdded() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService();
        final LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        final CountDownLatch keyLockedLatch = new CountDownLatch(1);
        final String key = "Key";
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                map.lock(key);
                keyLockedLatch.countDown();
                pause(20);
                // Don't add anything to the map just unlock
                map.unlock(key);
            }
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertNull( map.get(key) );
            } 
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
                assertFalse( itr.hasNext() );
            } 
        });
        
        executorService.shutdownWithin(1, TimeUnit.SECONDS);
        executorService.rethrow();
        
        assertTrue( map.isEmpty() );
    }
    
    @Test
    public void testClearWithlocks() throws InterruptedException {
        TestExecutorService executorService = new TestExecutorService();
        final LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        final CountDownLatch keyLockedLatch = new CountDownLatch(2);
        final String key1 = "Key-1";
        final String key2 = "Key-2";
        final String key3 = "Key-2";
        
        map.lock(key1);
        map.unlockWithNewValue(key1, 1);
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                map.lock(key2);
                keyLockedLatch.countDown();
                pause(20);
                map.put(key2, 2);
                map.unlock(key2);
            }
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                // Just lock and unlock key3 - never actually add it
                map.lock(key3);
                keyLockedLatch.countDown();
                pause(20);
                map.unlock(key3);
            }
        });
        
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    keyLockedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                map.clear();
            } 
        });
        
        executorService.shutdownWithin(1, TimeUnit.SECONDS);
        executorService.rethrow();
        
        assertTrue( map.isEmpty() );
    }
    
    @Test
    public void testLockingWithPut() throws InterruptedException {
        final int numThreads = 20;
        final int numIterations = 5000;
        final String baseKey = "Key";
        
        TestExecutorService executorService = new TestExecutorService();
        final LockingMap<String, Integer> map = new LockingMap<String, Integer>();
        final CyclicBarrier barrier = new CyclicBarrier(numThreads);
        
        for( int i = 0; i < numThreads; ++i ) {
            final int threadNumber = i;
            executorService.submit( new Runnable() {
                @Override
                public void run() {
                    for( int j = 0; j < numIterations; ++j ) {
                        try {
                            barrier.await(2, TimeUnit.SECONDS);
                        } catch (Exception e) {
                           throw new RuntimeException(e);
                        }
                        
                        String key = baseKey + "-" +  j;
                        
                        Integer current = map.lock(key);
                        if( ( j + threadNumber ) % 2 == 0 ) {
                            try {
                                if( current == null ) {
                                    map.put(key, 1);
                                } else {
                                    map.put(key, current + 1);
                                }
                            } finally {
                                map.unlock(key);
                            }
                        } else {
                            int newVal = -1;
                            try {
                                if( current == null ) {
                                    newVal = 1;
                                } else {
                                    newVal = current + 1;
                                }
                            } finally {
                                map.unlockWithNewValue(key, newVal );
                            }
                        }
                    }
                }
            });
        }
        
        executorService.shutdownWithin(30, TimeUnit.SECONDS);
        executorService.rethrow();
        
        for( int j = 0; j < numIterations; ++j ) {
            assertEquals( numThreads, map.get( baseKey + "-" + j ).intValue() );
        }
        
        // Count back down
        executorService = new TestExecutorService();
        
        for( int i = 0; i < numThreads; ++i ) {
            final int threadNumber = i;
            executorService.submit( new Runnable() {
                @Override
                public void run() {
                    for( int j = 0; j < numIterations; ++j ) {
                        try {
                            barrier.await(2, TimeUnit.SECONDS);
                        } catch (Exception e) {
                           throw new RuntimeException(e);
                        }
                        
                        String key = baseKey + "-" +  j;
                        
                        Integer current = map.lock(key);
                        if( ( j + threadNumber ) % 2 == 0 ) {
                            try {
                                assertNotNull( current );
                                if( current.intValue() == 1 ) {
                                    map.remove( key );
                                } else {
                                    map.put( key, current - 1 );
                                }
                            } finally {
                                map.unlock(key);
                            }
                        } else {
                            Integer newVal = current;
                            try {
                                assertNotNull( current );
                                if( current.intValue() == 1 ) {
                                    newVal = null;
                                } else {
                                    newVal = current - 1;
                                }
                            } finally {
                                map.unlockWithNewValue(key, newVal );
                            }
                        }
                    }
                }
            });
        }
        
        executorService.shutdownWithin(20, TimeUnit.SECONDS);
        executorService.rethrow();
        assertTrue( "Map is not empty:" + map.toString(), map.isEmpty() );
    }
}
