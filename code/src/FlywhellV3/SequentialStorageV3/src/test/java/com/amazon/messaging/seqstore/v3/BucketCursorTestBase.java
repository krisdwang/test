package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.channels.IllegalSelectorException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import lombok.Cleanup;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.store.BucketCursor;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.utils.Pair;


public abstract class BucketCursorTestBase extends TestCase {
    private final AckIdGenerator generator = new AckIdGenerator();
    
    private final Random random = new Random(0);
    
    public abstract BucketStore getBucketStore(String name) throws SeqStoreException;
    
    public abstract void closeBucketStore(BucketStore store) throws SeqStoreDatabaseException;
    
    /**
     * Insert a random set of entries into the bucket store and return a sorted map of 
     * those entries.
     * 
     * @param generator ack id generator
     * @param store    the bucket store to insert into
     * @param startRange  the earliest (inclusive) time to insert with
     * @param endRange    the max (exclusive) time to insert with
     * @param cnt         the number of entries to insert
     * @return          a sorted map of the entries inserted
     * @throws SeqStoreDatabaseException
     */
    private NavigableMap<AckIdV3, StoredEntry<AckIdV3>> insertRandomEntries(
            BucketStore store, int startRange, int endRange, int cnt)
            throws SeqStoreDatabaseException
    {
        NavigableMap< AckIdV3, StoredEntry<AckIdV3> > retval = new TreeMap< AckIdV3, StoredEntry<AckIdV3> >();
        int range = endRange - startRange;
        
        for( int i = 0; i < cnt; i++ ) {
            int time = random.nextInt( range ) + startRange;
            StoredEntry<AckIdV3> entry = insertMessage(store, time, i);
            retval.put( entry.getAckId(), entry );
        }
        
        return retval;
    }

    private StoredEntry<AckIdV3> insertMessage(BucketStore store, int time, int number)
            throws SeqStoreDatabaseException 
    {
        AckIdV3 ackId = generator.getAckId( time );
        StoredEntry<AckIdV3> entry = new StoredEntryV3( ackId, ( "Message" + number ).getBytes(), "Log" + number );
        store.put( ackId, entry, true );
        return entry;
    }
    
    private void assertStoredEntryEquals( StoredEntry<AckIdV3> expected, StoredEntry<AckIdV3> actual ) {
        assertNotNull( actual );
        assertEquals( expected, actual );
        assertArrayEquals( expected.getPayload(), actual.getPayload() );
    }
    
    private void assertIteratorsMatch( Iterator< StoredEntry<AckIdV3> > mapIter, BucketCursor cursor ) 
        throws SeqStoreDatabaseException 
    {
        while( mapIter.hasNext() ) {
            StoredEntry< AckIdV3> bucketEntry = cursor.nextEntry( null );
            StoredEntry<AckIdV3> mapEntry = mapIter.next();
            assertStoredEntryEquals( mapEntry, bucketEntry );
        }
    }

    private void assertCursorAtEndOfStore(BucketCursor cursor )
        throws SeqStoreDatabaseException 
    {
        assertNull( cursor.nextEntry( null ) );
        assertEquals( 0L, cursor.getDistance( new AckIdV3( Long.MAX_VALUE, true ) ) );
    }
    
    @Test
    public void testEmpty() throws SeqStoreException {
        BucketStore store = null;
        try {
            store = getBucketStore("testNext");
        
            @Cleanup
            BucketCursor cursor = store.createCursor();
            
            assertNull( cursor.nextEntry( null ) );
            assertEquals( 0L, cursor.getDistance( null ) );
            assertEquals( 0L, cursor.getDistance( new AckIdV3( 0, false ) ) );
            assertEquals( 0L, cursor.getDistance( new AckIdV3( Long.MAX_VALUE, true ) ) );
            assertFalse( cursor.jumpToKey( new AckIdV3( 5, false ) ) );
            assertEquals( 0L, cursor.advanceToKey( new AckIdV3( 5, false ) ) );
        } finally {
            closeBucketStore(store);
        }
    }
    
    @Test
    public void testSingle() throws SeqStoreDatabaseException, SeqStoreException, IOException {
        BucketStore store = null;
        try {
            store = getBucketStore("testSeek");
            
            StoredEntry<AckIdV3> entry = insertMessage(store, 5, 1);
            
            BucketCursor cursor = store.createCursor();
            try {
                assertStoredEntryEquals( entry, cursor.nextEntry( null ) );
            } finally {
                cursor.close();
            }
            
            assertEquals( entry.getAckId(), store.getFirstId() );
            assertEquals( entry.getAckId(), store.getLastId() );
            
            cursor = store.createCursor();
            try {
                assertEquals( null, cursor.nextEntry( new AckIdV3( 4, true ) ) );
                assertStoredEntryEquals( entry, cursor.nextEntry(  new AckIdV3( 5, true ) ) );
            } finally {
                cursor.close();
            }
            
            cursor = store.createCursor();
            try {
                assertFalse( cursor.jumpToKey( new AckIdV3( 4, true ) ) );
                assertEquals( null, cursor.current() );
                assertFalse( cursor.jumpToKey( new AckIdV3( 6, true ) ) );
                assertEquals( entry.getAckId(), cursor.current() );
                assertEquals( 0, cursor.advanceToKey( new AckIdV3( 6, true ) ) );
                assertEquals( entry.getAckId(), cursor.current() );
                assertEquals( null, cursor.nextEntry( null ) );
            } finally {
                cursor.close();
            }
            
            cursor = store.createCursor();
            try {
                assertEquals( 0, cursor.advanceToKey( new AckIdV3( 4, true ) ) );
                assertEquals( null, cursor.current() );
                assertEquals( 1, cursor.advanceToKey( new AckIdV3( 6, true ) ) );
                assertEquals( entry.getAckId(), cursor.current() );
                assertEquals( 0, cursor.advanceToKey( new AckIdV3( 6, true ) ) );
                assertEquals( entry.getAckId(), cursor.current() );
                assertEquals( null, cursor.nextEntry( null ) );
            } finally {
                cursor.close();
            }
        } finally {
            closeBucketStore(store);
        }
    }
    
    @Test
    public void testNext() throws SeqStoreException {
        BucketStore store = null;
        try {
            store = getBucketStore("testNext");
            
            @Cleanup
            BucketCursor cursor = store.createCursor();
            
            // Assert that an iterator on an empty bucket works
            assertCursorAtEndOfStore( cursor );
            
            SortedMap<AckIdV3, StoredEntry<AckIdV3>> entries = 
                insertRandomEntries( store, 1, 1000, 50 );
            
            // Assert that the iterator sees all the inserted entries
            assertIteratorsMatch( entries.values().iterator(), cursor );
            assertCursorAtEndOfStore( cursor );
    
            SortedMap<AckIdV3, StoredEntry<AckIdV3>> nextEntries = 
                insertRandomEntries( store, 1000, 2000, 50 );
            
            @Cleanup
            BucketCursor copiedCursor = cursor.copy();
            assertEquals( copiedCursor.current(), cursor.current() );
    
            // Assert that the iterator sees all the newly added values after it
            //  even if it had already been pushed to the end
            assertIteratorsMatch( nextEntries.values().iterator(), cursor );
            assertCursorAtEndOfStore( cursor );
            
            // Assert that the copy sees the same values
            assertIteratorsMatch( nextEntries.values().iterator(), copiedCursor );
            assertCursorAtEndOfStore( copiedCursor );
            
            // Assert that a new iterator created after all entries are added sees all entries
            @Cleanup
            BucketCursor bucketCursor2 = store.createCursor();
            assertIteratorsMatch( entries.values().iterator(), bucketCursor2 );
            assertIteratorsMatch( nextEntries.values().iterator(), bucketCursor2 );
            assertCursorAtEndOfStore( bucketCursor2 );
        } finally {
            closeBucketStore(store);
        }
    }
    
    @Test
    public void testDistance() throws SeqStoreException {
        BucketStore store = null;
        try {
            store = getBucketStore("testDistance");
            
            StoredEntry<AckIdV3> message1 = insertMessage( store, 1, 1);
            StoredEntry<AckIdV3> message2 = insertMessage( store, 2, 1);
            
            @Cleanup
            BucketCursor cursor = store.createCursor();
            
            assertEquals(0, cursor.getDistance(null));
            assertEquals(1, cursor.getDistance(message1.getAckId()));
            assertEquals(2, cursor.getDistance(message2.getAckId()));
                     
            assertStoredEntryEquals( message1, cursor.nextEntry( null ) );
            
            assertEquals(0, cursor.getDistance(null));
            assertEquals(0, cursor.getDistance(message1.getAckId()));
            assertEquals(1, cursor.getDistance(message2.getAckId()));
            
            assertStoredEntryEquals( message2, cursor.nextEntry( null ) );
            
            assertEquals(1, cursor.getDistance(null));
            assertEquals(1, cursor.getDistance(message1.getAckId()));
            assertEquals(0, cursor.getDistance(message2.getAckId()));
                
            assertNull( cursor.nextEntry( null ) );
            
            assertEquals(1, cursor.getDistance(null));
            assertEquals(1, cursor.getDistance(message1.getAckId()));
            assertEquals(0, cursor.getDistance(message2.getAckId()));
            
            StoredEntry<AckIdV3> message3 = insertMessage( store, 2, 1);
            assertStoredEntryEquals( message3, cursor.nextEntry( null ) );
            
            assertEquals(2, cursor.getDistance(null));
            assertEquals(2, cursor.getDistance(message1.getAckId()));
            assertEquals(1, cursor.getDistance(message2.getAckId()));
            assertEquals(0, cursor.getDistance(message3.getAckId()));
        } finally {
            closeBucketStore(store);
        }
    }
    
    @Test
    public void testNextWithMax() throws SeqStoreException {
        BucketStore store = null;
        try {
            store = getBucketStore("testNextWithMax");
            
            StoredEntry<AckIdV3> message1 = insertMessage( store, 1, 1);
            StoredEntry<AckIdV3> message2 = insertMessage( store, 2, 1);
            
            @Cleanup
            BucketCursor cursor = store.createCursor();
            
            assertNull( cursor.nextEntry( new AckIdV3( message1.getAckId(), false ) ) );
            
            assertEquals(0, cursor.getDistance(null));
            assertEquals(1, cursor.getDistance(message1.getAckId()));
            assertEquals(2, cursor.getDistance(message2.getAckId()));
            
            assertStoredEntryEquals( message1, cursor.nextEntry( message2.getAckId() ) );
            
            assertNull( cursor.nextEntry( new AckIdV3( message2.getAckId(), false ) ) );
            
            assertStoredEntryEquals( message2, cursor.nextEntry( new AckIdV3( 1000, false ) ) );
            
            assertNull( cursor.nextEntry( null ) );
            
            StoredEntry<AckIdV3> message3 = insertMessage( store, 2, 1);
            assertStoredEntryEquals( message3, cursor.nextEntry( new AckIdV3( 1000, false ) ) );
        } finally {
            closeBucketStore(store);
        }
     }
    
    /**
     * Select a random element from a set.
     * 
     * @param set the set to choose from
     * @return the item chosen and its location in iteration order in the set.
     */
    private <T> Pair<T, Integer> selectRandomElement( Set<T> set )
    {
        Iterator< T > itr = set.iterator();
        int pos = random.nextInt( set.size() ) + 1;
        for( int i = 0; i < pos - 1; ++i ) {
            itr.next();
        }
        
        return new Pair<T, Integer>( itr.next(), pos );
    }
    
    /**
     * Select a random location from the given set of keys. The location will be either
     * just before, exactly on, or just after a randomly chosen element in the set.
     * 
     * @param key
     * @return the chosen element and its distance from the start of the set (including itself).
     */
    private Pair<AckIdV3, Integer> selectRandomLocation( NavigableSet<AckIdV3> set )
    {
        Pair<AckIdV3, Integer> randomElement = selectRandomElement(set);
        
        int pos = randomElement.getValue();
        
        // possible positions relative to the chosen element:
        // -2 between it and the previous element, 
        // -1 immediately before it,
        // 0 exact match, 
        // 1 immediately after, 
        // 2 between it and the next element
        
        int relativePos;
        if( pos < 1 ) {
            relativePos = random.nextInt(4) - 1;
        } else if( pos < 2 ) {
            relativePos = random.nextInt(3);
        } else {
            relativePos = random.nextInt(5) - 2;
        }
        
        AckIdV3 ackId = randomElement.getKey();
        
        switch( relativePos ) {
        case -2:
            NavigableSet<AckIdV3> headSet = set.headSet( ackId, false );
            AckIdV3 previous = headSet.last();
            
            if( ackId.getTime() != previous.getTime() ) {
                // This will have a sequence higher than previous as it comes from the same generator
                return new Pair<AckIdV3, Integer>( generator.getAckId( ackId.getTime() - 1 ), pos - 1);                
            } else if( ackId.getSeq() != previous.getSeq() + 1 ) {
                return new Pair<AckIdV3, Integer>( generator.getAckId( ackId.getTime(), ackId.getSeq() - 1 ), pos - 1);
            } else {
                return new Pair<AckIdV3, Integer>(new AckIdV3( randomElement.getKey(), false), pos - 1);
            }
        case -1:
            return new Pair<AckIdV3, Integer>(new AckIdV3( randomElement.getKey(), false), pos - 1);
        case 0:
            return randomElement;
        case 1:
            return new Pair<AckIdV3, Integer>(new AckIdV3( randomElement.getKey(), true), pos );
        case 2:
            NavigableSet<AckIdV3> tailSet = set.tailSet( ackId, false );
            if( !tailSet.isEmpty() ) {
                AckIdV3 next = tailSet.last();
                
                if( next.getTime() != ackId.getTime() ) {
                    // This will have a sequence higher than ackId as it comes from the same generator
                    return new Pair<AckIdV3, Integer>( generator.getAckId( ackId.getTime() ), pos );
                } else if( ackId.getSeq() + 1 != next.getSeq() ) {
                    return new Pair<AckIdV3, Integer>( generator.getAckId( ackId.getTime(), ackId.getSeq() + 1 ), pos );
                } else {
                    return new Pair<AckIdV3, Integer>(new AckIdV3( randomElement.getKey(), true), pos);
                }
            } else {
                // This will have a sequence higher than ackId as it comes from the same generator
                return new Pair<AckIdV3, Integer>( generator.getAckId( ackId.getTime() ), pos );
            }
        default:
            throw new IllegalSelectorException();
        }
    }
    
    /**
     * Choose a random message in the entries map and advance to it. Assert that the position
     * of itr matches that of the entry.
     * <p>
     * This function requires that itr be before any entry in entries.
     * 
     * @param entries the entries to choose from
     * @param cursor the cursor to advance. Must start before all of entries
     * @return the ack id that was selected
     * @throws BucketDeletedException 
     * @throws SeqStoreDatabaseException 
     */
    private AckIdV3 advanceToRandomMessage(NavigableMap<AckIdV3, StoredEntry<AckIdV3>> entries,
                                           BucketCursor cursor, boolean jump)
            throws SeqStoreDatabaseException 
    {
        if( entries.isEmpty() ) throw new IllegalArgumentException("entries cannot be empty");
        
        Pair<AckIdV3, Integer> randomElement = selectRandomLocation(entries.navigableKeySet());

        AckIdV3 destKey = randomElement.getKey();
        
        if( jump ) {
            boolean keyExists = cursor.jumpToKey( destKey );
            assertEquals( entries.containsKey( destKey.getCoreAckId() ), keyExists );
        } else {
            long distanceTravelled = cursor.advanceToKey( destKey );
            assertEquals( randomElement.getValue().longValue(), distanceTravelled );
        }
        
        assertEquals( randomElement.getValue().longValue() - 1, cursor.getDistance( new AckIdV3( entries.firstKey(), false ) ) );
        assertEquals( entries.size() - randomElement.getValue().longValue(), cursor.getDistance( new AckIdV3( entries.lastKey(), true ) ) );
        
        NavigableSet<AckIdV3> subset = entries.navigableKeySet().headSet( destKey, true );
        assertEquals( subset.last(), cursor.current() );
        
        return cursor.current();
    }
    
    @Test
    public void testSeek() throws SeqStoreDatabaseException, SeqStoreException, IOException {
        BucketStore store = null;
        try {
            store = getBucketStore("testSeek");
            
            @Cleanup
            BucketCursor cursor = store.createCursor();
            
            NavigableMap<AckIdV3, StoredEntry<AckIdV3>> entries = 
                insertRandomEntries( store, 1, 1000, 200 );
            
            boolean jump = random.nextBoolean();
            advanceToRandomMessage( entries, cursor, jump );
            
            AckIdV3 pastEnd = generator.getAckId( 2000 );
            if( jump ) {
                assertFalse( cursor.jumpToKey( pastEnd ) );
            } else {
                cursor.advanceToKey( pastEnd );
            }
            assertEquals( entries.lastKey(), cursor.current() );
            assertEquals( entries.size() - 1, cursor.getDistance( null ) );
            
            NavigableMap<AckIdV3, StoredEntry<AckIdV3>> newEntries = 
                insertRandomEntries( store, 1000, 2000, 100 );
            
            NavigableMap<AckIdV3, StoredEntry<AckIdV3>> remainingEntries = newEntries;
            while( !remainingEntries.isEmpty() ) {
                AckIdV3 reached = advanceToRandomMessage(
                        remainingEntries, cursor, random.nextBoolean() );
                remainingEntries = remainingEntries.tailMap(reached, false);
            }
        } finally {
            closeBucketStore(store);
        }
    }
}
