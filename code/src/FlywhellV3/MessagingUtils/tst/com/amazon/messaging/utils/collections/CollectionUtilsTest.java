package com.amazon.messaging.utils.collections;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import static org.junit.Assert.*;


public class CollectionUtilsTest {
    private final class EvenFirstComparator implements Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            int i1 = o1.intValue();
            int i2 = o2.intValue();
            if( i1 % 2 == 0 ) {
                if( i2 % 2 != 0 ) return -1;
            } else if( i2 % 2 == 0 ) {
                return 1;
            }
            
            return i1 - i2;
        }
    }

    @Test
    public void testBasic() {
        TreeSet<Integer> set1 = new TreeSet<Integer>( Arrays.asList( 1, 3, 5, 7, 9 ) );
        TreeSet<Integer> set2 = new TreeSet<Integer>( Arrays.asList( 2, 4, 6, 8, 10 ) );
        TreeSet<Integer> set3 = new TreeSet<Integer>( Arrays.asList( 2, 5, 9, 11 ) );
        
        @SuppressWarnings("unchecked")
        NavigableSet<Integer> result = CollectionUtils.mergeSets( Arrays.asList( set1, set2, set3 ) );
        
        Iterator<Integer> itr = result.iterator();
        for( int i = 1; i <= 11; ++i ) {
            assertTrue( itr.hasNext() );
            assertEquals( i, itr.next().intValue() );
        }
        assertFalse( itr.hasNext() );
    }
    
    @Test
    public void testDuplicates() {
        TreeSet<Integer> set1 = new TreeSet<Integer>( Arrays.asList( 2, 3, 4, 5, 10 ) );
        TreeSet<Integer> set2 = new TreeSet<Integer>( Arrays.asList( 1, 2, 3, 4, 5, 11 ) );
        TreeSet<Integer> set3 = new TreeSet<Integer>( Arrays.asList( 3, 4, 5, 12 ) );
        
        @SuppressWarnings("unchecked")
        NavigableSet<Integer> result = CollectionUtils.mergeSets( Arrays.asList( set1, set2, set3 ) );
        
        Iterator<Integer> itr = result.iterator();
        for( int i = 1; i <= 5; ++i ) {
            assertTrue( itr.hasNext() );
            assertEquals( i, itr.next().intValue() );
        }
        for( int i = 10; i <= 12; ++i ) {
            assertTrue( itr.hasNext() );
            assertEquals( i, itr.next().intValue() );
        }
        assertFalse( itr.hasNext() );
    }
    
    @Test
    public void testNoSets() {
        NavigableSet<Integer> result = CollectionUtils.mergeSets( Collections.<SortedSet<Integer>>emptyList() );
        assertTrue( result.isEmpty() );
    }
    
    @Test
    public void testCustomCompare() {
        Comparator<Integer> evenFirst = new EvenFirstComparator();
        
        TreeSet<Integer> set1 = new TreeSet<Integer>( evenFirst );
        set1.addAll( Arrays.asList( 2, 6, 1, 5 ) );
        TreeSet<Integer> set2 = new TreeSet<Integer>( evenFirst );
        set2.addAll(  Arrays.asList( 2, 3, 7 ) );
        TreeSet<Integer> set3 = new TreeSet<Integer>( evenFirst );
        set3.addAll( Arrays.asList( 8, 4, 5 ) );
        
        @SuppressWarnings("unchecked")
        NavigableSet<Integer> result = CollectionUtils.mergeSets( Arrays.asList( set1, set2, set3 ) );
        
        Iterator<Integer> itr = result.iterator();
        for( int i = 2; i <= 8; i += 2 ) {
            assertTrue( itr.hasNext() );
            assertEquals( i, itr.next().intValue() );
        }
        
        for( int i = 1; i <= 7; i += 2 ) {
            assertTrue( itr.hasNext() );
            assertEquals( i, itr.next().intValue() );
        }
        
        assertFalse( itr.hasNext() );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMismatchCompare() {
        TreeSet<Integer> set1 = new TreeSet<Integer>();
        set1.addAll( Arrays.asList( 2, 6, 1, 5 ) );
        TreeSet<Integer> set2 = new TreeSet<Integer>( new EvenFirstComparator() );
        set2.addAll(  Arrays.asList( 2, 3, 7 ) );
        
        try {
            CollectionUtils.mergeSets( Arrays.asList( set1, set2 ) );
            fail( "Mismatched compare function not detected.");
        } catch( IllegalArgumentException e ) {
        }
    }
}
