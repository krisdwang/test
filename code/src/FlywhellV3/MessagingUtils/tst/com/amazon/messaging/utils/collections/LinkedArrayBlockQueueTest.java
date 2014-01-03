package com.amazon.messaging.utils.collections;



import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import com.amazon.messaging.utils.collections.LinkedArrayBlockQueue.Removable;



public class LinkedArrayBlockQueueTest {

    /**
     * Tests the boundary conditions where there's only one element that's being added and removed, to ensure that
     * elements that are at the beginning or the end of the block are treated correctly.
     */
    @Test
    public void singleItemAtATimeAddRemoveTest () {
        LinkedArrayBlockQueue<Object> queue = new LinkedArrayBlockQueue<Object> ();

        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE + 1; i++) {
            Object o1 = new Object ();

            Removable r = queue.offer (o1);
            assertEquals ("Object and head of the queue should match (" + i + ")", o1, queue.peek ());

            r.remove ();
            assertNull ("Head should be null.", queue.peek ());
        }
    }
    
    /**
     * Tests case where one it is added and then removed with poll
     */
    @Test
    public void singleItemAtATimeAddPollTest () {
        LinkedArrayBlockQueue<Object> queue = new LinkedArrayBlockQueue<Object> ();

        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE + 1; i++) {
            Object o1 = new Object ();

            queue.offer (o1);
            assertEquals ("Object and head of the queue should match (" + i + ")", o1, queue.peek ());

            Object removed = queue.poll();
            assertEquals( o1, removed );
            assertNull ("Head should be null.", queue.peek ());
        }
    }
    
    /**
     * Tests that if the first entry is removed the second entry is returned correctly
     */
    @Test
    public void removeFirstEntryThenPollTest () {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();
        Removable r = queue.offer(0);
        queue.offer(1);
        
        r.remove();
        
        queue.offer(2);
        
        assertEquals( Integer.valueOf( 1 ), queue.poll() );
        assertEquals( Integer.valueOf( 2 ), queue.poll() );
        assertTrue( queue.isEmpty() );
        assertNull( queue.poll() );
    }
    
    /**
     * Tests that if the a middle entry is removed the entries around it are returned
     * correctly
     */
    @Test
    public void removeMiddleEntryThenPollTest () {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();
        
        queue.offer(0);
        Removable r = queue.offer(1);
        queue.offer(2);
        
        r.remove();
        
        queue.offer(3);
        
        assertEquals( Integer.valueOf( 0 ), queue.poll() );
        assertEquals( Integer.valueOf( 2 ), queue.poll() );
        assertEquals( Integer.valueOf( 3 ), queue.poll() );
        assertTrue( queue.isEmpty() );
        assertNull( queue.poll() );
    }
    
    /**
     * Tests that if the list entry is removed then the previous entries are 
     * returned correctly
     */
    @Test
    public void removeLastEntryThenPollTest () {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();
        
        queue.offer(0);
        queue.offer(1);
        Removable r = queue.offer(2);
        
        r.remove();
        
        queue.offer(3);
        
        assertEquals( Integer.valueOf( 0 ), queue.poll() );
        assertEquals( Integer.valueOf( 1 ), queue.poll() );
        assertEquals( Integer.valueOf( 3 ), queue.poll() );
        assertTrue( queue.isEmpty() );
        assertNull( queue.poll() );
    }

    /**
     * Tests the boundary conditions, when there are one more than the block size elements (i.e., a block switch).
     */
    @Test
    public void removeEntireBlockTest () {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();

        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; i++) {
            queue.offer (i);
        }

        Integer o1 = LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE+1;
        Removable r = queue.offer (o1);

        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; i++) {
            Integer removed = queue.poll ();
            assertEquals( Integer.valueOf(i), removed );
        }

        assertEquals ("Object and head of the queue should match.", o1, queue.peek ());

        r.remove ();
        assertNull ("Head should be null.", queue.peek ());
    }
    
    /**
     * Test calling removal twice.
     */
    @Test
    public void callRemoveTwiceTest () {
        LinkedArrayBlockQueue<Object> queue = new LinkedArrayBlockQueue<Object> ();

        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; i++) {
            queue.offer (new Object ());
        }

        Object o1 = new Object ();
        Removable r = queue.offer (o1);

        assertTrue ("First remove should succeed.", r.remove ());
        assertFalse ("Second call to remove should be false.", r.remove ());
    }

    /**
     * Tests calling remove for the element, which was returned by poll.
     */
    @Test
    public void callRemoveAfterPollTest () {
        LinkedArrayBlockQueue<Object> queue = new LinkedArrayBlockQueue<Object> ();

        List<Removable> removables = new ArrayList<Removable> ();
        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; i++) {
            removables.add (queue.offer (new Object ()));
        }

        // Remove all elements through poll
        //
        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; i++) {
            assertNotNull (queue.poll ());
        }
        assertNull (queue.poll ());

        for (Removable r : removables) {
            assertFalse (r.remove ());
        }
    }
    
    @Test
    public void removeRandomTest() {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();

        int queueSize = (LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 3) + 2;
        Set<Integer> expectedEntries = new HashSet<Integer>();
        List<Removable> removables = new ArrayList<Removable> ();
        for (int i = 0; i < queueSize; i++) {
            removables.add (queue.offer (i));
            expectedEntries.add( i );
        }
        
        Random random = new Random(5);
        for( int i = 0; i < queueSize; ++i ) {
            Integer removed = queue.takeRandom(random);
            assertNotNull( "Got null after only " + i + " entries", removed );
            assertTrue( expectedEntries.remove( removed ) );
            assertEquals( queueSize - i - 1, queue.size() );
        }
        
        Integer removed = queue.takeRandom(random);
        assertNull( removed );
        assertEquals( 0, queue.size() );
        
        // Try adding a new entry to make sure that works
        Removable r = queue.offer (queueSize+1);
        assertEquals ("Object and head of the queue should match (" + queueSize + 1 + ")", 
                Integer.valueOf( queueSize + 1 ), queue.peek ());
        r.remove ();
        assertNull ("Head should be null.", queue.peek ());
    }
    
    @Test
    public void getRandomTest() {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();

        int queueSize = (LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 3) + 2;
        Set<Integer> expectedEntries = new HashSet<Integer>();
        List<Removable> removables = new ArrayList<Removable> ();
        for (int i = 0; i < queueSize; i++) {
            removables.add (queue.offer (i));
            expectedEntries.add( i );
        }
        
        Random random = new Random(5);
        for( int i = 0; i < 10 * queueSize; ++i ) {
            Integer entry = queue.getRandom(random);
            assertNotNull( "Got null after only " + i + " entries", entry );
            assertTrue( expectedEntries.contains( entry ) );
        }
        assertEquals( queueSize, queue.size() );
        
        for( int i = 0; i < queueSize; ++i ) {
            assertEquals( Integer.valueOf( i ), queue.poll() );
        }
        assertTrue( queue.isEmpty() );
    }
    
    /**
     * Tests calling remove for the element, which was returned by takeRandom.
     */
    @Test
    public void callRemoveAfterTakeRandomTest () {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();

        int queueSize = (LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 3) + 2;
        Map<Integer, Removable> expectedEntries = new HashMap<Integer, LinkedArrayBlockQueue.Removable>();
        for (int i = 0; i < queueSize; i++) {
            expectedEntries.put (i, queue.offer (i));
        }
        
        Random random = new Random(5);
        for( int i = 0; i < queueSize; ++i ) {
            Integer removed = queue.takeRandom(random);
            assertNotNull( "Got null after only " + i + " entries", removed );
            Removable  r = expectedEntries.remove(removed);
            assertNotNull( r );
            assertFalse (r.remove ());
        }
       
        assertNull (queue.poll ());
    }
    
    /**
     * Tests the removing a middle block from the queue.
     */
    @Test
    public void removeMiddleBlockTest () {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();

        List<Removable> removables = new ArrayList<Removable> ();
        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 3; i++) {
            removables.add( queue.offer (i) );
        }
        
        for( int i = LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 2 + 1; ++i ) {
            boolean removed = removables.get(i).remove();
            assertTrue( removed );
        }
        
        // Test that poll works for the remaining entries
        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; ++i ) {
            Integer removed = queue.poll();
            assertEquals( Integer.valueOf(i), removed );
        }
        
        for (int i = 2 * LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE + 1; i < 3 * LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; ++i ) {
            Integer removed = queue.poll();
            assertEquals( Integer.valueOf(i), removed );
        }
    }
    
    /**
     * Tests the removing the entire newest block from the queue
     */
    @Test
    public void removeNewestBlockTest () {
        LinkedArrayBlockQueue<Integer> queue = new LinkedArrayBlockQueue<Integer> ();

        List<Removable> removables = new ArrayList<Removable> ();
        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 2; i++) {
            removables.add( queue.offer (i) );
        }
        
        for( int i = LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 2; ++i ) {
            boolean removed = removables.get(i).remove();
            assertTrue( removed );
        }
        
        // Add a new entry
        removables.add( queue.offer (LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 2) );
        
        // Test that poll works for the remaining entries
        for (int i = 0; i < LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE; ++i ) {
            Integer removed = queue.poll();
            assertEquals( Integer.valueOf(i), removed );
        }
        
        Integer removed = queue.poll();
        assertEquals( Integer.valueOf(LinkedArrayBlockQueue.DEFAULT_BLOCK_SIZE * 2), removed );
        
        assertTrue( queue.isEmpty() );
    }
}
