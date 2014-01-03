package com.amazon.messaging.seqstore.v3.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.mapPersistence.MapBucketStore;
import com.amazon.messaging.testing.TestExecutorService;


public class OpenBucketStoreTrackerTest {
    private MapBucketStore createStore() {
        AckIdV3 bucketId = new AckIdV3( 5, false);
        ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> map 
            = new ConcurrentSkipListMap<AckIdV3, StoredEntry<AckIdV3>>();
        return new MapBucketStore( bucketId, 1, map );
    }
    
    @Test
    public void testEmpty() {
        OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        assertEquals( 0, tracker.openBucketCount() );
        assertEquals( 0, tracker.openBucketStoreCount() );
        assertTrue( tracker.getOpenBucketStores().isEmpty() );
        assertFalse( isDNEBucketMarkedOpen(tracker) );
    }
    
    private void testCollection( Collection<MapBucketStore> collection, MapBucketStore...expectedBucketStores ) {
        Set<MapBucketStore> expectedSet = new HashSet<MapBucketStore>( Arrays.asList( expectedBucketStores ) );
        assertEquals( expectedSet.size(), collection.size() );
        
        Iterator<MapBucketStore> itr = collection.iterator();
        for( int i = 0; i < expectedSet.size(); ++i ) {
            assertTrue( itr.hasNext() );
            MapBucketStore store = itr.next();
            assertTrue( 
                    store + " was not in " + expectedBucketStores,
                    expectedSet.contains( store ) );
        }
        assertFalse( itr.hasNext() );
        try {
            itr.next();
            fail( "next() after then end of the collection did not fail.");
        } catch( NoSuchElementException e ) {
            // expected
        }
    }
    
    @Test
    public void testAdd() {
        OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        StoreId storeIdB1 = new StoreIdImpl("B1");
        MapBucketStore bucketStoreB1 = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1 );
        assertEquals( 1, tracker.openBucketCount() );
        assertEquals( 1, tracker.openBucketStoreCount() );
        assertFalse( tracker.getOpenBucketStores().isEmpty() );
        assertTrue( hasOpenBucketWithMatchingId(tracker, storeIdB1, bucketStoreB1) );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB1 );
        
        StoreId storeIdB2 = new StoreIdImpl("B2");
        MapBucketStore bucketStoreB2 = createStore();
        tracker.addBucketStore( storeIdB2, bucketStoreB2 );
        
        assertEquals( 2, tracker.openBucketCount() );
        assertEquals( 2, tracker.openBucketStoreCount() );
        assertFalse( tracker.getOpenBucketStores().isEmpty() );
        assertTrue( hasOpenBucketWithMatchingId(tracker, storeIdB2, bucketStoreB2) );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB1, bucketStoreB2 );
    }

    private boolean hasOpenBucketWithMatchingId(
        OpenBucketStoreTracker<MapBucketStore> tracker, StoreId storeId, MapBucketStore bucketStore)
    {
        return tracker.hasOpenStoreForBucket(storeId, bucketStore.getBucketId());
    }
    
    private boolean isDNEBucketMarkedOpen(
        OpenBucketStoreTracker<MapBucketStore> tracker)
    {
        return tracker.hasOpenStoreForBucket(
                new StoreIdImpl("DNE", null), new AckIdV3( 5, false ) );
    }
    
    @Test
    public void testAddDuplicate() {
        OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        StoreId storeIdB1 = new StoreIdImpl("B1");
        MapBucketStore bucketStoreB1 = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1 );
        
        try {
            tracker.addBucketStore( storeIdB1, bucketStoreB1 );
            fail( "Should not have been able to add the same store twice.");
        } catch( IllegalArgumentException e ) {
            // Sucess
        }
        
        // Metrics shouldn't have changed
        assertEquals( 1, tracker.openBucketCount() );
        assertEquals( 1, tracker.openBucketStoreCount() );
        assertFalse( tracker.getOpenBucketStores().isEmpty() );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB1 );
    }
    
    @Test
    public void testRemove() {
        OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        StoreId storeIdB1 = new StoreIdImpl("B1");
        MapBucketStore bucketStoreB1 = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1 );
        
        StoreId storeIdB2 = new StoreIdImpl("B2");
        MapBucketStore bucketStoreB2 = createStore();
        tracker.addBucketStore( storeIdB2, bucketStoreB2 );
        
        tracker.removeBucketStore(storeIdB1, bucketStoreB1);
        
        assertEquals( 1, tracker.openBucketCount() );
        assertEquals( 1, tracker.openBucketStoreCount() );
        assertFalse( tracker.getOpenBucketStores().isEmpty() );
        assertFalse( hasOpenBucketWithMatchingId( tracker, storeIdB1, bucketStoreB1 ) );
        assertTrue( hasOpenBucketWithMatchingId( tracker, storeIdB2, bucketStoreB2 ) );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB2 );
    }
    
    @Test
    public void testAddSameBucket() {
        OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        StoreId storeIdB1 = new StoreIdImpl("B1");
        MapBucketStore bucketStoreB1a = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1a );
        
        MapBucketStore bucketStoreB1b = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1b );
        
        assertEquals( 1, tracker.openBucketCount() );
        assertEquals( 2, tracker.openBucketStoreCount() );
        assertFalse( tracker.getOpenBucketStores().isEmpty() );
        assertTrue( hasOpenBucketWithMatchingId( tracker, storeIdB1, bucketStoreB1a ) );
        assertFalse( isDNEBucketMarkedOpen(tracker) );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB1a, bucketStoreB1b );
    }
    
    @Test
    public void testRemoveSameBucket() {
        OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        StoreId storeIdB1 = new StoreIdImpl("B1");
        MapBucketStore bucketStoreB1a = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1a );
        
        MapBucketStore bucketStoreB1b = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1b );
        
        tracker.removeBucketStore( storeIdB1, bucketStoreB1b);
        
        assertEquals( 1, tracker.openBucketCount() );
        assertEquals( 1, tracker.openBucketStoreCount() );
        assertFalse( tracker.getOpenBucketStores().isEmpty() );
        assertTrue( hasOpenBucketWithMatchingId( tracker, storeIdB1, bucketStoreB1a) );
        assertFalse( isDNEBucketMarkedOpen(tracker) );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB1a );
    }
    
    @Test
    public void testClear() {
        OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        StoreId storeIdB1 = new StoreIdImpl("B1");
        MapBucketStore bucketStoreB1 = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1 );
        StoreId storeIdB2 = new StoreIdImpl("B2");
        MapBucketStore bucketStoreB2a = createStore();
        tracker.addBucketStore( storeIdB2, bucketStoreB2a );
        MapBucketStore bucketStoreB2b = createStore();
        tracker.addBucketStore( storeIdB2, bucketStoreB2b );
        
        assertEquals( 2, tracker.openBucketCount() );
        assertEquals( 3, tracker.openBucketStoreCount() );
        
        tracker.clear();
        
        assertEquals( 0, tracker.openBucketCount() );
        assertEquals( 0, tracker.openBucketStoreCount() );
        assertTrue( tracker.getOpenBucketStores().isEmpty() );
        assertFalse( hasOpenBucketWithMatchingId( tracker, storeIdB1, bucketStoreB1) );
        assertFalse( hasOpenBucketWithMatchingId( tracker, storeIdB2, bucketStoreB2a) );
    }
    
    @Test
    public void testRemoveNeverAddded() {
        OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        
        StoreId storeIdB1 = new StoreIdImpl("B1");
        StoreId storeIdB2 = new StoreIdImpl("B2");
        StoreId storeIdB3 = new StoreIdImpl("B3");
        MapBucketStore bucketStoreB1 = createStore();
        tracker.addBucketStore( storeIdB1, bucketStoreB1 );
        MapBucketStore bucketStoreB2a = createStore();
        tracker.addBucketStore( storeIdB2, bucketStoreB2a );
        MapBucketStore bucketStoreB2b = createStore();
        tracker.addBucketStore( storeIdB2, bucketStoreB2b );
        
        
        try {
            tracker.removeBucketStore( storeIdB3, createStore( ) );
            fail( "Removing a store that was never added should throw");
        } catch( IllegalArgumentException e ) {
            // success
        }
        
        assertEquals( 2, tracker.openBucketCount() );
        assertEquals( 3, tracker.openBucketStoreCount() );
        assertTrue( hasOpenBucketWithMatchingId( tracker, storeIdB1, bucketStoreB1) );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB1, bucketStoreB2a, bucketStoreB2b );
        
        // Now try removing a store where the bucket has a store, but not the one that is being removed
        try {
            tracker.removeBucketStore( storeIdB1, createStore( ) );
            fail( "Removing a store that was never added should throw");
        } catch( IllegalArgumentException e ) {
            // success
        }
        
        assertEquals( 2, tracker.openBucketCount() );
        assertEquals( 3, tracker.openBucketStoreCount() );
        assertTrue( hasOpenBucketWithMatchingId( tracker, storeIdB1, bucketStoreB1) );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB1, bucketStoreB2a, bucketStoreB2b );
        
        // Now try removing a store where the bucket has multiple stores, but not the one that is being removed
        try {
            tracker.removeBucketStore( storeIdB2, createStore( ) );
            fail( "Removing a store that was never added should throw");
        } catch( IllegalArgumentException e ) {
            // success
        }
        
        assertEquals( 2, tracker.openBucketCount() );
        assertEquals( 3, tracker.openBucketStoreCount() );
        assertTrue( hasOpenBucketWithMatchingId( tracker, storeIdB1, bucketStoreB1) );
        testCollection( tracker.getOpenBucketStores(), bucketStoreB1, bucketStoreB2a, bucketStoreB2b );
    }
    
    @Test
    public void testMultiThreaded() throws InterruptedException {
        final Random random = new Random(5);
        final int numBuckets = 10;
        final int numThreads = 4;
        final int numIterations = 50000;

        final AtomicBoolean abort = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(numThreads);
        final OpenBucketStoreTracker<MapBucketStore> tracker = new OpenBucketStoreTracker<MapBucketStore>();
        
        TestExecutorService executor = new TestExecutorService();
        try {
            for( int i = 0; i < numThreads; ++i ) {
                executor.submit( new Runnable() {
                    @Override
                    public void run() {
                        boolean success = false;
                        try {
                            barrier.await();
                            for( int j = 0; !abort.get() && j < numIterations; ++j ) {
                                String bucketName = "bucket-" + random.nextInt( numBuckets );
                                StoreId storeId = new StoreIdImpl( bucketName, null );
                                MapBucketStore mapBucket = createStore();
                                tracker.addBucketStore(storeId, mapBucket);
                                assertTrue( hasOpenBucketWithMatchingId(tracker, storeId, mapBucket) );
                                tracker.removeBucketStore( storeId, mapBucket);
                            }
                        } catch( Exception e ) {
                            throw new RuntimeException(e);
                        } finally {
                            if( !success ) abort.set(true);
                        }
                    }
                });
            }
            
            executor.shutdownWithin(15, TimeUnit.SECONDS);
            executor.rethrow();
            
            assertEquals( 0, tracker.openBucketCount() );
            assertEquals( 0, tracker.openBucketStoreCount() );
            assertTrue( tracker.getOpenBucketStores().isEmpty() );
        } finally {
            executor.shutdownNow();
            executor.shutdownWithin(1, TimeUnit.SECONDS);
        }
        
        
    }
}
