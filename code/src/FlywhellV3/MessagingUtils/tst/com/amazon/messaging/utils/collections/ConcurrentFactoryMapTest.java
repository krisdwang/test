package com.amazon.messaging.utils.collections;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import lombok.Getter;


public class ConcurrentFactoryMapTest extends FactoryMapTestBase {
    @Override
    protected <K, V> Map<K, V> getFactoryMap(ValueFactory<V> factory) {
        return new ConcurrentFactoryMap<K, V>( new ConcurrentHashMap<K, V>(), factory, false );
    }

    @Override
    protected <K, V> Map<K, V> getFactoryMap(DependentValueFactory<K, V> factory) {
        return new ConcurrentFactoryMap<K, V>( new ConcurrentHashMap<K, V>(), factory, false );
    }
    
    private static class RecordingToStringFactory implements DependentValueFactory<Integer, String> {
        @Getter
        private ConcurrentMap< Integer, AtomicInteger > createCount = new ConcurrentHashMap<Integer, AtomicInteger>();
        
        @Override
        public String createValue(Integer key) {
            createCount.putIfAbsent( key, new AtomicInteger() );
            createCount.get( key ).incrementAndGet();
            Thread.yield();
            
            return key.toString();
        }
    }
    
    private static class MapGetter extends Thread {
        private final Map<Integer, String> map;
        private final CountDownLatch latch;
        private final int start;
        private final int stop;
        
        @Getter
        private Throwable exception;
        
        public MapGetter( Map<Integer, String> map, CountDownLatch latch, int start, int stop ) {
            this.map = map;
            this.latch = latch;
            this.start = start;
            this.stop = stop;
            
            setUncaughtExceptionHandler( new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    exception = e;
                }
                
            } );
        }
        
        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException( "Interrupted", e );
            }
            for( int i = start; i < stop; ++i ) {
                map.get( i );
            }
        }
    }
    
    private void doInsertion( Map< Integer, String > map, int numThreads ) throws InterruptedException {
        List< MapGetter > threads = new ArrayList<MapGetter>();
        CountDownLatch latch = new CountDownLatch(1);
        
        for( int i = 0; i < numThreads; ++i ) {
            MapGetter thread = new MapGetter( map, latch, i * 10, i * 10 + 20 ); 
            thread.start();
            threads.add( thread );
        }
        latch.countDown();
        
        for( MapGetter thread : threads ) {
            thread.join();
            if( thread.getException() != null ) {
                throw new AssertionError( thread.getException() );
            }
        }
        
        assertEquals( numThreads * 10 + 10, map.size() );
        for( int i = 0; i < numThreads * 10 + 10 ; ++i ) {
            assertTrue( map.containsKey( i ) );
            assertEquals( Integer.toString( i ), map.get( i ) );
        }
    }
    
    @Test
    public void testNonUniqueCreationFactoryMap() throws InterruptedException {
        RecordingToStringFactory valueFactory = new RecordingToStringFactory();
        
        Map< Integer, String > map = new ConcurrentFactoryMap<Integer, String>( 
                new ConcurrentHashMap<Integer, String>(), valueFactory, false );
        
        doInsertion(map, 200);
        
        // There should be some concurrency
        int extraCreates = calculateExtraCreates(valueFactory);
        assertTrue( extraCreates > 0 );
        System.out.println( "TestConcurrentFactoryMap.testNonUniqueCreationFactoryMap: Create called " + extraCreates + " extra times." );
    }

    private int calculateExtraCreates(RecordingToStringFactory valueFactory) {
        int overCount = 0;
        for( AtomicInteger count : valueFactory.getCreateCount().values() ) {
            if( count.get() > 1 ) overCount++; 
        }
        return overCount;
    }
    
    @Test
    public void testUniqueCreationFactoryMap() throws InterruptedException {
        RecordingToStringFactory valueFactory = new RecordingToStringFactory();
        
        Map< Integer, String > map = new ConcurrentFactoryMap<Integer, String>( 
                new ConcurrentHashMap<Integer, String>(), valueFactory, true );
        
        doInsertion(map, 200);
        
        // There should be no duplicate creates
        assertEquals( 0, calculateExtraCreates(valueFactory) );
    }
}
