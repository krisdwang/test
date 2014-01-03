package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import lombok.Cleanup;

import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.Bucket;
import com.amazon.messaging.seqstore.v3.store.BucketImpl;
import com.amazon.messaging.seqstore.v3.store.BucketIterator;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.store.StorePosition;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;

public abstract class BucketIteratorTestBase extends TestCase {

    private static final StoreIdImpl STORE_NAME = new StoreIdImpl("BucketTest", "");
    
    private static final Random random = new Random( 0 );
    
    private static final AckIdGenerator generator = new AckIdGenerator();
    
    private final BucketStorageType storageType;
    
    public BucketIteratorTestBase(BucketStorageType storageType) {
        this.storageType = storageType;
    }

    protected abstract StorePersistenceManager createPersistenceManager()
        throws SeqStoreException, IOException;
    
    private Bucket createBucket(StorePersistenceManager manager) throws SeqStoreException {
        AckIdV3 bucketId = new AckIdV3( 1, true );
        return BucketImpl.createNewBucket(STORE_NAME, bucketId, storageType, manager);
    }
    
    private void assertStoredEntryEquals( StoredEntry<AckIdV3> expected, StoredEntry<AckIdV3> actual ) {
        assertNotNull( actual );
        assertEquals( expected, actual );
        assertArrayEquals( expected.getPayload(), actual.getPayload() );
    }

    /**
     * Insert a random set of entries into the bucket and return a sorted map of 
     * those entries.
     * 
     * @param generator ack id generator
     * @param bucket    the bucket to insert into
     * @param startRange  the earliest (inclusive) time to insert with
     * @param endRange    the max (exclusive) time to insert with
     * @param cnt         the number of entries to insert
     * @return          a sorted map of the entries inserted
     * @throws SeqStoreDatabaseException
     */
    private NavigableMap<AckIdV3, StoredEntry<AckIdV3>> insertRandomEntries(
            Bucket bucket, int startRange, int endRange, int cnt)
            throws SeqStoreDatabaseException
    {
        NavigableMap< AckIdV3, StoredEntry<AckIdV3> > retval = new TreeMap< AckIdV3, StoredEntry<AckIdV3> >();
        int range = endRange - startRange;
        
        for( int i = 0; i < cnt; i++ ) {
            AckIdV3 ackId = generator.getAckId( random.nextInt( range ) + startRange );
            StoredEntry<AckIdV3> entry = new StoredEntryV3( ackId, ( "Message" + i ).getBytes(), "Log" + i );
            retval.put( ackId, entry );
            bucket.put( ackId, entry, true );
        }
        
        return retval;
    }
    
    private void assertIteratorsMatch( Iterator< StoredEntry<AckIdV3> > mapIter, BucketIterator bucketItr ) 
        throws SeqStoreDatabaseException 
    {
        long cnt = bucketItr.getPosition().getPosition();
        while( mapIter.hasNext() ) {
            StoredEntry< AckIdV3> bucketEntry = bucketItr.next();
            StoredEntry<AckIdV3> mapEntry = mapIter.next();
            assertStoredEntryEquals( mapEntry, bucketEntry );
            ++cnt;
            assertEquals( new StorePosition( bucketItr.getBucketId(), new AckIdV3( mapEntry.getAckId(), cnt ) ), bucketItr.getPosition() );
        }
    }
    
    private void assertIteratorAtEndOfBucket( Bucket bucket, BucketIterator bucketItr )
        throws SeqStoreDatabaseException 
    {
        assertNull( bucketItr.next() );
        assertEquals( bucket.getEntryCount(), bucketItr.getPosition().getPosition() );
    }
        
    @Test
    public void testIteratorNext()
            throws InvalidConfigException, SeqStoreException, IOException
    {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager();
        @Cleanup
        Bucket bucket = createBucket(manager);
        
        BucketIterator bucketItr = bucket.getIter();
        
        // Assert that an iterator on an empty bucket works
        assertEquals( bucket.getBucketId(), bucketItr.getPosition().getBucketId() );
        assertIteratorAtEndOfBucket( bucket, bucketItr );
        
        SortedMap<AckIdV3, StoredEntry<AckIdV3>> entries = 
            insertRandomEntries( bucket, 1, 1000, 50 );
        
        // Assert that the iterator sees all the inserted entries
        assertIteratorsMatch( entries.values().iterator(), bucketItr );
        assertIteratorAtEndOfBucket( bucket, bucketItr );

        SortedMap<AckIdV3, StoredEntry<AckIdV3>> nextEntries = 
            insertRandomEntries( bucket, 1000, 2000, 50 );
        
        BucketIterator copiedItr = bucket.copyIter( bucketItr );
        assertEquals( copiedItr.getPosition(), bucketItr.getPosition() );

        // Assert that the iterator sees all the newly added values after it
        //  even if it had already been pushed to the end
        assertIteratorsMatch( nextEntries.values().iterator(), bucketItr );
        assertIteratorAtEndOfBucket( bucket, bucketItr );
        
        // Assert that the copy sees the same values
        assertIteratorsMatch( nextEntries.values().iterator(), copiedItr );
        assertIteratorAtEndOfBucket( bucket, copiedItr );
        
        // Assert that a new iterator created after all entries are added sees all entries
        BucketIterator bucketItr2 = bucket.getIter();
        assertEquals( bucket.getBucketId(), bucketItr2.getPosition().getBucketId() );
        assertIteratorsMatch( entries.values().iterator(), bucketItr2 );
        assertIteratorsMatch( nextEntries.values().iterator(), bucketItr2 );
        assertIteratorAtEndOfBucket( bucket, bucketItr2 );
    }
    
    /**
     * Test that an iterator can iterate over previously inserted values even if later 
     * inserts are still occurring. Also checks that the later inserts will show up for that
     * iterator after they are complete.
     *  
     * @throws InvalidConfigException
     * @throws SeqStoreException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testIteratorNextWithParallelInserts()
            throws InvalidConfigException, SeqStoreException, IOException, InterruptedException
    {
        @Cleanup
        final StorePersistenceManager manager = createPersistenceManager();
        @Cleanup
        final Bucket bucket = createBucket(manager);
        
        final SortedMap<AckIdV3, StoredEntry<AckIdV3>> entries = 
            insertRandomEntries( bucket, 1, 1000, 1000 );
        
        final CyclicBarrier startBarrier = new CyclicBarrier(2);
        final CyclicBarrier insertsCompleteBarrier = new CyclicBarrier(2);
        
        final SortedMap<AckIdV3, StoredEntry<AckIdV3>> newEntries = 
            Collections.synchronizedSortedMap( new TreeMap<AckIdV3, StoredEntry<AckIdV3>>() );
        
        TestExecutorService testExector = new TestExecutorService("testIteratorNextWithParallelInserts");
        testExector.submit( new Callable<Void>() {
            public Void call() throws Exception {
                startBarrier.await();
                SortedMap<AckIdV3, StoredEntry<AckIdV3>> addedEntries = 
                    insertRandomEntries( bucket, 1000, 2000, 1000 );
                newEntries.putAll( addedEntries );
                insertsCompleteBarrier.await();
                return null;
            }
        });
        
        testExector.submit( new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                startBarrier.await();
                BucketIterator bucketItr = bucket.getIter();
                assertIteratorsMatch( entries.values().iterator(), bucketItr );
                insertsCompleteBarrier.await();
                assertIteratorsMatch( newEntries.values().iterator(), bucketItr );
                assertIteratorAtEndOfBucket(bucket, bucketItr);
                return null;
            }
        });

        testExector.shutdownWithin( 4, TimeUnit.SECONDS );
        testExector.rethrowLastAssertion();
        testExector.rethrowLastUncaughtException();
    }
    
    /**
     * Choose a random message in the entries map and advance to it. Assert that the position
     * of itr matches that of the entry.
     * <p>
     * This function requires that itr be before any entry in entries.
     * 
     * @param entries the entries to choose from
     * @param itr the iterator to advance. Must start before all of entries
     * @param entriesOffset how far entries is into the bucket.
     * @return the ack id that was selected
     * @throws BucketDeletedException 
     * @throws SeqStoreDatabaseException 
     */
    private AckIdV3 advanceToRandomMessage(NavigableMap<AckIdV3, StoredEntry<AckIdV3>> entries,
                                           BucketIterator itr, long entriesOffset,
                                           boolean usePosition)
            throws SeqStoreDatabaseException 
    {
        Iterator< StoredEntry<AckIdV3> > mapIter = entries.values().iterator();
        int pos = random.nextInt( entries.size() ) + 1;
        for( int i = 0; i < pos - 1; ++i ) {
            mapIter.next();
        }
        
        AckIdV3 base = mapIter.next().getAckId();
        
        int posAdjustment;
        Boolean inclusive;
        Long basePos = null;
        AckIdV3 expected;
        
        int relativePos = random.nextInt(3);
        if(relativePos == 0 ) {
            inclusive = null; 
            posAdjustment = 0;
            expected = base;
        } else if( relativePos == 1 && pos > 1 ) {
            inclusive = false;
            posAdjustment = -1;
            expected = entries.navigableKeySet().lower( base );
        } else {
            inclusive = true;
            posAdjustment = 0;
            expected = base;
        }
        
        
        if( usePosition ) basePos = pos + entriesOffset;
        
        AckIdV3 ackIdToAdvanceTo = new AckIdV3( base, inclusive, basePos );
        AckIdV3 reached = itr.advanceTo( ackIdToAdvanceTo );
        
        assertEquals( expected, reached );
        assertEquals( pos + entriesOffset + posAdjustment, itr.getPosition().getPosition() );
        
        return reached;
    }
    
    private void testSeek( boolean jump ) throws SeqStoreDatabaseException, SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager();
        @Cleanup
        Bucket bucket = createBucket(manager);
        
        BucketIterator bucketItr = bucket.getIter();
        
        NavigableMap<AckIdV3, StoredEntry<AckIdV3>> entries = 
            insertRandomEntries( bucket, 1, 1000, 50 );
        
        advanceToRandomMessage( entries, bucketItr, 0, jump );
        
        AckIdV3 pastEnd = generator.getAckId( 2000 );
        AckIdV3 reached = bucketItr.advanceTo( pastEnd );
        assertEquals( entries.lastKey(), reached );
        assertEquals( entries.size(), bucketItr.getPosition().getPosition() );
        
        NavigableMap<AckIdV3, StoredEntry<AckIdV3>> newEntries = 
            insertRandomEntries( bucket, 1000, 2000, 100 );
        
        assertEquals( entries.size(), bucketItr.getPosition().getPosition() );
        
        reached = advanceToRandomMessage( newEntries, bucketItr, entries.size(), jump );
        while( bucketItr.getPosition().getPosition() != bucket.getEntryCount() ) {
            AckIdV3 afterReached = new AckIdV3( reached, true ) ;
            reached = advanceToRandomMessage(
                    newEntries.tailMap( afterReached, true ), 
                    bucketItr, 
                    entries.size() + newEntries.headMap(afterReached).size(),
                    jump );
        }
    }
    
    @Test
    public void testAdvanceTo()
            throws InvalidConfigException, SeqStoreException, IOException
    {
        testSeek( false );
    }
    
    @Test
    public void testJumpTo()
            throws InvalidConfigException, SeqStoreException, IOException
    {
        testSeek( true );
    }
    
    private void checkDeleteDetected( BucketIterator itr ) throws SeqStoreDatabaseException {
        assertNull( "Iterator was not aware the bucket was deleted.", itr.next() );
        assertEquals( "Iterator was not aware the bucket was deleted.", 
                itr.currentKey(), itr.advanceTo( generator.getAckId( 2000 ) ) );
    }
    
    @Test
    public void testDeletedBucket() throws InvalidConfigException, SeqStoreException, IOException {
        final int numEntries = 50;
        
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager();
        Bucket bucket = createBucket(manager);
        
        boolean doneDelete = false;
        try {
            insertRandomEntries( bucket, 1, 1000, numEntries );
            
            BucketIterator itr = bucket.getIter();
            
            assertNotNull(itr.next() );
            assertNotNull(itr.next() );
            assertNotNull(itr.next() );
            
            BucketIterator copy = bucket.copyIter( itr );
            
            bucket.deleted();
            manager.deleteBucketStore( STORE_NAME, bucket.getBucketId() );
            doneDelete = true;
            
            checkDeleteDetected( itr );
            checkDeleteDetected( copy );
        } finally {
            if( !doneDelete ) bucket.close();
        }
    }
    
    private void checkCloseDetected( Bucket bucket, BucketIterator itr )
        throws SeqStoreDatabaseException 
    {
        try {
            itr.getPosition();
            fail( "Iterator was not aware the bucket was closed.");
        } catch( IllegalStateException ex ) {
            // Success
        }
        
        try {
            itr.next();
            fail( "Iterator was not aware the bucket was closed.");
        } catch( IllegalStateException ex ) {
            // Success
        }
        
        try {
            itr.advanceTo( generator.getAckId( 2000 ) );
            fail( "Iterator was not aware the bucket was closed.");
        } catch( IllegalStateException ex ) {
            // Success
        }
        
        try {
            bucket.copyIter(itr);
            fail( "Iterator was not aware the bucket was closed.");
        } catch( IllegalStateException ex ) {
            // Success
        }
    }
    
    @Test
    public void testClosedBucket() throws InvalidConfigException, SeqStoreException, IOException {
        final int numEntries = 50;
        
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager();
        Bucket bucket = createBucket(manager);
        
        boolean doneClose = false;
        try {
            insertRandomEntries( bucket, 1, 1000, numEntries );
            
            BucketIterator itr = bucket.getIter();
            
            assertNotNull(itr.next() );
            assertNotNull(itr.next() );
            assertNotNull(itr.next() );
            
            BucketIterator copy1 = bucket.copyIter( itr );
            
            bucket.close();
            doneClose = true;
            
            checkCloseDetected( bucket, itr );
            checkCloseDetected( bucket, copy1 );
        } finally {
            if( !doneClose ) bucket.close();
        }
    }

}
 