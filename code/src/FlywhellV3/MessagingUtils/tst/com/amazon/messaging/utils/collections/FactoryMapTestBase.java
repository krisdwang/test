package com.amazon.messaging.utils.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;

import org.junit.Test;

public abstract class FactoryMapTestBase {
    protected abstract <K,V> Map<K,V> getFactoryMap( ValueFactory< V > factory );
    protected abstract <K,V> Map<K,V> getFactoryMap( DependentValueFactory< K, V > factory );
    
    @Data
    protected static class CountingValueFactory implements ValueFactory<Integer> {
        private final AtomicInteger counter = new AtomicInteger(0);
        
        @Override
        public Integer createValue() {
            return counter.incrementAndGet();
        }
    }
    
    @Test
    public void testValueFactory() {
        CountingValueFactory valueFactory = new CountingValueFactory();
        
        Map< String, Integer > map = getFactoryMap( valueFactory );
        
        map.put( "existing1", 100 );
        map.put( "existing2", 101 );

        // Check that manually inserted items appear in the map
        //  and items never inserted do not appear
        assertEquals( 2, map.size() );
        assertEquals( 100, map.get( "existing1").intValue() );
        assertEquals( 101, map.get( "existing2").intValue() );
        assertTrue( map.containsKey( "existing1" ) );
        assertTrue( map.containsKey( "existing2" ) );
        assertFalse( map.containsKey( "dne1" ) );
        assertTrue( map.containsValue( 100 ) );
        assertTrue( map.containsValue( 101 ) );
        assertFalse( map.containsValue( 1 ) );
        
        // Check that not inserted values are correctly created from the factory
        assertEquals( 1, map.get( "dne1" ).intValue() );
        assertEquals( 2, map.get( "dne2" ).intValue() );
        
        assertEquals( 4, map.size() );
        assertEquals( 100, map.get( "existing1").intValue() );
        assertEquals( 101, map.get( "existing2").intValue() );
        assertEquals( 1, map.get( "dne1").intValue() );
        assertEquals( 2, map.get( "dne2").intValue() );
        assertTrue( map.containsKey( "existing1" ) );
        assertTrue( map.containsKey( "existing2" ) );
        assertTrue( map.containsKey( "dne1" ) );
        assertTrue( map.containsKey( "dne2" ) );
        assertTrue( map.containsValue( 100 ) );
        assertTrue( map.containsValue( 101 ) );
        assertTrue( map.containsValue( 1 ) );
        assertTrue( map.containsValue( 2 ) );
        assertFalse( map.containsValue( 3 ) );
        
        map.remove( "existing1" );
        map.remove( "dne2" );
        
        // Test that removal worked
        assertEquals( 2, map.size() );
        assertEquals( 101, map.get( "existing2").intValue() );
        assertEquals( 1, map.get( "dne1").intValue() );
        assertFalse( map.containsKey( "existing1" ) );
        assertTrue( map.containsKey( "existing2" ) );
        assertTrue( map.containsKey( "dne1" ) );
        assertFalse( map.containsKey( "dne2" ) );
        assertFalse( map.containsValue( 100 ) );
        assertTrue( map.containsValue( 101 ) );
        assertTrue( map.containsValue( 1 ) );
        assertFalse( map.containsValue( 2 ) );
        
        // Check that retrieving removed value from the map creates new values
        //  using the factory
        assertEquals( 3, map.get( "existing1" ).intValue() );
        assertEquals( 4, map.get( "dne2" ).intValue() );
        
        assertEquals( 4, map.size() );
        assertEquals( 3, map.get( "existing1").intValue() );
        assertEquals( 101, map.get( "existing2").intValue() );
        assertEquals( 1, map.get( "dne1").intValue() );
        assertEquals( 4, map.get( "dne2").intValue() );
        assertTrue( map.containsKey( "existing1" ) );
        assertTrue( map.containsKey( "existing2" ) );
        assertTrue( map.containsKey( "dne1" ) );
        assertTrue( map.containsKey( "dne2" ) );
        assertTrue( map.containsValue( 3 ) );
        assertTrue( map.containsValue( 101 ) );
        assertTrue( map.containsValue( 1 ) );
        assertTrue( map.containsValue( 4 ) );
        assertFalse( map.containsValue( 5 ) );
        
        assertEquals( 4, valueFactory.getCounter().get() );
    }

    protected static class ToStringFactory implements DependentValueFactory<Integer, String> {
        @Override
        public String createValue(Integer key) {
            return key.toString();
        }
    }
    
    @Test
    public void testDependentValueFactory() {
        ToStringFactory valueFactory = new ToStringFactory();
        
        Map< Integer, String > map = getFactoryMap( valueFactory );
        
        map.put( 1, "one" );
        map.put( 2, "two" );
         
        assertEquals( 2, map.size() );
        assertEquals( "one", map.get( 1 ) );
        assertEquals( "two", map.get( 2 ) );
        assertEquals( "3", map.get( 3 ) );
        assertEquals( "4", map.get( 4 ) );
        assertEquals( 4, map.size() );
        
        assertFalse( map.containsKey( 5 ) );
        assertEquals( "5", map.get( 5 ) );
        assertTrue( map.containsKey( 5 ) );
    }
}
