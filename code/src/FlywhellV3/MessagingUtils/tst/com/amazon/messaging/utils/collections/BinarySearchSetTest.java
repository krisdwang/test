package com.amazon.messaging.utils.collections;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import org.junit.Test;

import static org.junit.Assert.*;


public class BinarySearchSetTest {
    private void validateIteration(  Iterator<Integer> itr, int start, int end, int increment ) {
        if( start < end ) {
            for( int i = start; i <= end; i += increment ) {
                assertTrue( itr.hasNext() );
                assertEquals( i, itr.next().intValue() );
            }
        } else {
            for( int i = start; i >= end; i += increment ) {
                assertTrue( itr.hasNext() );
                assertEquals( i, itr.next().intValue() );
            }
        }
    }
    
    private void testEndOfIteration( Iterator<Integer> itr ) {
        try {
            itr.next();
            fail( "Did not throw NoSuchElementException when out of elements.");
        } catch( NoSuchElementException e ) {
            // Success
        }
    }
    
    @Test
    public void testIteration() {
        NavigableSet<Integer> test = new BinarySearchSet<Integer>(
                Arrays.asList( 1, 2, 3, 4, 5 ) );
        
        Iterator<Integer> itr = test.iterator();
        validateIteration(itr, 1, 5, 1);
        testEndOfIteration( itr );
        
        itr = test.descendingIterator();
        validateIteration( itr, 5, 1, -1 );
        testEndOfIteration( itr );
        
        test = test.descendingSet();
        itr = test.iterator();
        validateIteration(itr, 5, 1, -1 );
        testEndOfIteration( itr );
        
        itr = test.descendingIterator();
        validateIteration( itr, 1, 5, 1 );
        testEndOfIteration( itr );
    }
    
    @Test
    public void testSize() {
        NavigableSet<Integer> test1 = new BinarySearchSet<Integer>(
                Arrays.asList( 1, 2, 3, 4, 5 ) );
        
        assertEquals( 5, test1.size() );
        assertFalse( test1.isEmpty() );
        
        assertEquals( 5, test1.descendingSet().size() );
        assertFalse( test1.descendingSet().isEmpty() );
        
        NavigableSet<Integer> test2 = new BinarySearchSet<Integer>(Arrays.asList( new Integer[0]));
        assertEquals( 0, test2.size() );
        assertTrue( test2.isEmpty() );
        
        assertEquals( 0, test2.descendingSet().size() );
        assertTrue( test2.descendingSet().isEmpty() );
    }
    
    @Test
    public void testToArray() {
        Integer []testArray = new Integer[] { 1, 2, 3, 4, 5, };
        
        NavigableSet<Integer> test1 = new BinarySearchSet<Integer>(
                Arrays.asList( testArray ) );
        
        Integer[] intResult = test1.toArray( new Integer[0]);
        assertArrayEquals( testArray, intResult );
        
        intResult = test1.toArray( new Integer[5]);
        assertArrayEquals( testArray, intResult );
        
        intResult = test1.toArray( new Integer[7]);
        for( int i = 0; i < 5; ++i ) {
            assertEquals( i + 1, intResult[i].intValue());
        }
        for( int i = 5; i < 7; ++i ) {
            assertNull(intResult[i]);
        }
        
        Object[] objResult = test1.toArray();
        assertArrayEquals( testArray, objResult );
    }
    
    @Test
    public void testSetEquals() {
        NavigableSet<Integer> test = new BinarySearchSet<Integer>(
                Arrays.asList( 1, 2, 3, 4, 5 ) );
        
        NavigableSet<Integer> test2 = new BinarySearchSet<Integer>(
                Arrays.asList( 1, 2, 3, 4, 5 ) );
        
        assertTrue( test.equals( test2 ) );
        assertTrue( test.descendingSet().equals( test2 ) );
        assertTrue( test.descendingSet().equals( test2.descendingSet() ) );
        assertTrue( test.equals( test2.descendingSet() ) );
        
        NavigableSet<Integer> treeSet = new TreeSet<Integer>();
        for( int i = 1; i <=5; ++i ) {
            treeSet.add( i );
        }
        assertTrue( treeSet.equals( test2 ) );
        assertTrue( test2.equals( treeSet ) );
        
        assertEquals( test, test.descendingSet().descendingSet() );
    }
    
    @Test
    public void testSubset() {
        NavigableSet<Integer> test = new BinarySearchSet<Integer>(
                Arrays.asList( 1, 3, 5, 7, 9 ) );
        
        NavigableSet<Integer> treeSet = new TreeSet<Integer>( test );
        for( int i = 1; i <= 9; i += 2 ) {
            treeSet.add( i );
        }
        
                                                     // From      , To
        testSubSetWithReverse(-1, 0, treeSet, test); // Before Set, Before Set
        testSubSetWithReverse(0, 5, treeSet, test);  // Before set, In set
        testSubSetWithReverse(0, 4, treeSet, test);  // Before set, Not in set
        testSubSetWithReverse(1, 5, treeSet, test);  // In set    , In set
        testSubSetWithReverse(1, 6, treeSet, test);  // In set    , Not in set
        testSubSetWithReverse(2, 5, treeSet, test);  // Not in set, In Set
        testSubSetWithReverse(2, 6, treeSet, test);  // Not In set, In Set
        testSubSetWithReverse(5, 10, treeSet, test); // In Set    , After Set
        testSubSetWithReverse(6, 10, treeSet, test); // Not In Set, After Set
        testSubSetWithReverse(10, 12, treeSet, test);// After Set , After Set
        
        testSubSetWithReverse(6, 6, treeSet, test); // Empty Set
        testSubSetWithReverse(5, 5, treeSet, test); // Max One Element
        
        testHeadSet( 0, treeSet, test );
        testHeadSet( 1, treeSet, test );
        testHeadSet( 3, treeSet, test );
        testHeadSet( 9, treeSet, test );
        testHeadSet( 10, treeSet, test );
        
        testTailSet(0, treeSet, test);
        testTailSet(1, treeSet, test);
        testTailSet(7, treeSet, test);
        testTailSet(9, treeSet, test);
        testTailSet(10, treeSet, test);
    }
    
    private void testSubSetWithReverse(int fromElement, int toElement, NavigableSet<Integer> reference,
                                       NavigableSet<Integer> test)
    {
        testSubSet( fromElement, toElement, reference, test );
        testSubSet( toElement, fromElement, reference.descendingSet(), test.descendingSet() );
    }

    private void testSubSet(int fromElement, int toElement, NavigableSet<Integer> reference,
                            NavigableSet<Integer> test) 
    {
        assertEquals( reference.subSet( fromElement, true, toElement, false), test.subSet( fromElement, true, toElement, false ) );
        assertEquals( reference.subSet( fromElement, true, toElement, true), test.subSet( fromElement, true, toElement, true ) );
        assertEquals( reference.subSet( fromElement, false, toElement, true), test.subSet( fromElement, false, toElement, true ) );
        assertEquals( reference.subSet( fromElement, false, toElement, false), test.subSet( fromElement, false, toElement, false ) );
        assertEquals( reference.subSet( fromElement, toElement ), test.subSet( fromElement, toElement ) );
    }
    
    private void testHeadSet(int start, NavigableSet<Integer> reference, NavigableSet<Integer> test) {
        assertEquals( reference.headSet( start, false ), test.headSet( start, false ) );
        assertEquals( reference.headSet( start, true ), test.headSet( start, true ) );
        assertEquals( reference.headSet( start ), test.headSet( start  ) );
        
        NavigableSet<Integer> revRef = reference.descendingSet();
        NavigableSet<Integer> revTest = test.descendingSet();
        assertEquals( revRef.headSet( start, false ), revTest.headSet( start, false ) );
        assertEquals( revRef.headSet( start, true ), revTest.headSet( start, true ) );
        assertEquals( revRef.headSet( start ), revTest.headSet( start  ) );
    }
    
    private void testTailSet(int end, NavigableSet<Integer> reference, NavigableSet<Integer> test) {
        assertEquals( reference.tailSet( end, false ), test.tailSet( end, false ) );
        assertEquals( reference.tailSet( end, true ), test.tailSet( end, true ) );
        assertEquals( reference.tailSet( end ), test.tailSet( end  ) );
        
        NavigableSet<Integer> revRef = reference.descendingSet();
        NavigableSet<Integer> revTest = test.descendingSet();
        assertEquals( revRef.tailSet( end, false ), revTest.tailSet( end, false ) );
        assertEquals( revRef.tailSet( end, true ), revTest.tailSet( end, true ) );
        assertEquals( revRef.tailSet( end ), revTest.tailSet( end  ) );
    }
    
    @Test
    public void testSearch() {
        NavigableSet<Integer> test = new BinarySearchSet<Integer>(
                Arrays.asList( 1, 3, 5, 7, 9 ) );
        
        NavigableSet<Integer> reverse = test.descendingSet();
        
        assertTrue( test.contains( 1 ) );
        assertTrue( reverse.contains( 1 ) );
        assertTrue( test.contains( 5 ) );
        assertTrue( reverse.contains( 5 ) );
        assertTrue( test.contains( 9 ) );
        assertTrue( reverse.contains( 9 ) );
        
        
        assertFalse( test.contains( 0 ) );
        assertFalse( reverse.contains( 0 ) );
        assertFalse( test.contains( 2 ) );
        assertFalse( reverse.contains( 2 ) );
        assertFalse( test.contains( 10 ) );
        assertFalse( reverse.contains( 10 ) );
        
        assertEquals( 1 , test.first().intValue() );
        assertEquals( 9 , test.last().intValue() );
        
        assertEquals( 9 , reverse.first().intValue() );
        assertEquals( 1 , reverse.last().intValue() );
        
        // Test the main set
        assertNull( test.floor( 0 ) );
        assertEquals( 1, test.floor(1).intValue() );
        assertEquals( 1, test.floor(2).intValue() );
        assertEquals( 9, test.floor(9).intValue() );
        assertEquals( 9, test.floor(10).intValue() );
        
        assertNull( test.lower( 0 ) );
        assertNull( test.lower( 1 ) );
        assertEquals( 1, test.lower(2).intValue() );
        assertEquals( 7, test.lower(9).intValue() );
        assertEquals( 9, test.lower(10).intValue() );
        
        assertEquals( 1, test.ceiling( 0 ).intValue() );
        assertEquals( 1, test.ceiling(1).intValue() );
        assertEquals( 3, test.ceiling(2).intValue() );
        assertEquals( 9, test.ceiling(9).intValue() );
        assertNull( test.ceiling(10) );
        
        assertEquals( 1, test.higher( 0 ).intValue() );
        assertEquals( 3, test.higher(1).intValue() );
        assertEquals( 3, test.higher(2).intValue() );
        assertNull( test.higher(9) );
        assertNull( test.higher(10) );
        
        // Test the reversed set
        assertNull( reverse.floor( 10 ) );
        assertEquals( 9, reverse.floor(9).intValue() );
        assertEquals( 9, reverse.floor(8).intValue() );
        assertEquals( 1, reverse.floor(1).intValue() );
        assertEquals( 1, reverse.floor(0).intValue() );
        
        assertNull( reverse.lower( 10 ) );
        assertNull( reverse.lower( 9 ) );
        assertEquals( 9, reverse.lower(7).intValue() );
        assertEquals( 3, reverse.lower(1).intValue() );
        assertEquals( 1, reverse.lower(0).intValue() );
        
        assertEquals( 9, reverse.ceiling( 10 ).intValue() );
        assertEquals( 9, reverse.ceiling(9).intValue() );
        assertEquals( 7, reverse.ceiling(8).intValue() );
        assertEquals( 1, reverse.ceiling(1).intValue() );
        assertNull( reverse.ceiling(0) );
        
        assertEquals( 9, reverse.higher( 10 ).intValue() );
        assertEquals( 7, reverse.higher(9).intValue() );
        assertEquals( 5, reverse.higher(7).intValue() );
        assertNull( reverse.higher(1) );
        assertNull( reverse.higher(0) );
    }
    
    @Test
    public void testEmptySearch() {
        NavigableSet<Integer> test = new BinarySearchSet<Integer>(
                Arrays.asList( new Integer[0] ) );
        
        NavigableSet<Integer> reverse = test.descendingSet();
        
        assertFalse( test.contains( 1 ) );
        assertFalse( reverse.contains( 1 ) );
        
        try {
            test.first();
            fail( "Empty set didn't throw.");
        } catch( NoSuchElementException e ) {
        }
        
        try {
            test.last();
            fail( "Empty set didn't throw.");
        } catch( NoSuchElementException e ) {
        }
        
        try {
            reverse.first();
            fail( "Empty set didn't throw.");
        } catch( NoSuchElementException e ) {
        }
        
        try {
            reverse.last();
            fail( "Empty set didn't throw.");
        } catch( NoSuchElementException e ) {
        }
        
        assertNull( test.floor( 1 ) );
        assertNull( test.lower( 1 ) );
        assertNull( test.ceiling(1) );
        assertNull( test.higher(1) );
        
        assertNull( reverse.floor( 1 ) );
        assertNull( reverse.lower( 1 ) );
        assertNull( reverse.ceiling(1) );
        assertNull( reverse.higher(1) );
    }
    
    @Test
    public void testCustomComparator() {
        Comparator<Integer> evenFirst = new Comparator<Integer>() {
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
        };
        
        List<Integer> testList = Arrays.asList( 2, 4, 6, 1, 3, 5 );
        NavigableSet<Integer> test = new BinarySearchSet<Integer>( testList, evenFirst );
        
        Iterator<Integer> itr = test.iterator();
        for( Integer i : testList ) {
            assertTrue( itr.hasNext() );
            assertEquals( i, itr.next() );
        }
        assertFalse( itr.hasNext() );
        
        
        NavigableSet<Integer> treeSet = new TreeSet<Integer>();
        treeSet.addAll( testList );
        assertTrue( treeSet.equals( test ) );
        assertTrue( treeSet.descendingSet().equals( test.descendingSet() ) );

        assertTrue( test.contains( 2 ) );
        assertFalse( test.contains( 8 ) );
        assertFalse( test.contains( 9 ) );
        
        assertTrue( test.comparator().compare(2, 1) < 0 );
        assertTrue( test.descendingSet().comparator().compare(2, 1) > 0 );
    }
    
    @Test
    public void testToString() {
        NavigableSet<Integer> test = new BinarySearchSet<Integer>(
                Arrays.asList( 1, 2, 3, 4, 5 ) );
        
        NavigableSet<Integer> treeSet = new TreeSet<Integer>();
        for( int i = 1; i <=5; ++i ) {
            treeSet.add( i );
        }
        
        assertEquals( test.toString(), treeSet.toString() );
        
        NavigableSet<Integer> empty = new BinarySearchSet<Integer>(
                Arrays.asList( new Integer[0] ) );
        assertEquals( "[]", empty.toString() );
    }
    
    @Test
    public void testReadOnly() {
        NavigableSet<Integer> test = new BinarySearchSet<Integer>(
                Arrays.asList( 1, 3, 5, 7, 9 ) );
        
        testReadOnly(test);
        testReadOnly(test.descendingSet());
    }

    private void testReadOnly(NavigableSet<Integer> test) {
        try {
            test.add( 3 );
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
        
        try {
            test.remove( 3 );
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
        
        try {
            test.addAll( Arrays.asList( 3 ) );
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
        
        try {
            test.removeAll( Arrays.asList( 3 ) );
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
        
        try {
            test.retainAll( Arrays.asList( 3 ) );
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
        
        try {
            test.clear();
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
        
        try {
            test.pollFirst();
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
        
        try {
            test.pollLast();
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
        
        try {
            Iterator<Integer> itr = test.iterator();
            assertTrue( itr.hasNext() );
            itr.next();
            itr.remove();
            fail( "Modify didn't fail");
        } catch( UnsupportedOperationException e ) {
        }
            
    }
    
    
}

