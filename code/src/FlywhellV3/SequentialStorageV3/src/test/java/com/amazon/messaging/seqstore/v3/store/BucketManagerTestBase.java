package com.amazon.messaging.seqstore.v3.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Assume;
import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.StoredCount;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.DefaultAckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.utils.collections.BinarySearchSet;

public abstract class BucketManagerTestBase {
    private static final String TEST_PAYLOAD = "message - test";
    
    private static final long TEST_ENTRY_SIZE = TEST_PAYLOAD.getBytes().length + AckIdV3.LENGTH;
    
    protected static final BucketStoreConfig DEDICATED_BUCKET_CONFIGURATION = 
            new BucketStoreConfig.Builder()
                .withMinPeriod(15)
                .withMaxEnqueueWindow(10)
                .withMaxPeriod(60)
                .withMinSize(2)
                .build();
    
    protected static final BucketStoreConfig SHARED_BUCKET_CONFIGURATION = 
            new BucketStoreConfig.Builder()
                .withMinPeriod(10)
                .withMaxEnqueueWindow(15)
                .withMaxPeriod(600)
                .withMinSize(1)
                .build();
    
    protected static final int TRANSITION_TO_DEDICATED_BUCKET_KB = 5;
    
    protected static final SeqStoreImmutableConfig STORE_CONFIG;
    
    static {
        SeqStoreConfig tmpConfig = new SeqStoreConfig();
        tmpConfig.setDedicatedDBBucketStoreConfig(DEDICATED_BUCKET_CONFIGURATION);
        tmpConfig.setSharedDBBucketStoreConfig(SHARED_BUCKET_CONFIGURATION);
        tmpConfig.setMaxSizeInKBBeforeUsingDedicatedDBBuckets(TRANSITION_TO_DEDICATED_BUCKET_KB);
        STORE_CONFIG = tmpConfig.getImmutableConfig();
    }

    protected static final int MAX_MSG = 2000;

    protected static final int MAX_DEQ_TIME = 15 * DEDICATED_BUCKET_CONFIGURATION.getMinPeriod() * 1000;

    protected final AckIdGenerator ackIdGen = new AckIdGenerator();

    protected final Random random = new Random(10);

    protected final AckIdV3 MAX_ID = ackIdGen.getAckId(MAX_DEQ_TIME, 0);

    protected final AckIdV3 MIN_ID = ackIdGen.getAckId(0, 0);

    protected abstract BucketManager createBucketManager(Clock clock,
            boolean clearExistingStore) throws IOException, SeqStoreException;

    protected abstract void shutdownBucketManager(BucketManager bm) throws SeqStoreException;
    
    /**
     * Get the persistence manager used by the bucket manager.
     */
    protected abstract StorePersistenceManager getPersistenceManager();
    
    protected final boolean useLLMEnv;
    
    protected BucketManagerTestBase(boolean useLLMEnv) {
        if( !Logger.getRootLogger().getAllAppenders().hasMoreElements() ) {
            Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout("%d\t%-5p [%t]: %m%n")));
        }
        this.useLLMEnv = useLLMEnv;
    }
    
    protected static BucketStoreConfig getConfig(BucketStorageType storageType) {
        if( storageType.isDedicated() ) {
            return DEDICATED_BUCKET_CONFIGURATION;
        } else {
            return SHARED_BUCKET_CONFIGURATION;
        }
    }
    
    protected BucketStoreConfig getDefaultConfig() {
       return getConfig(BucketStorageType.SharedDatabase);
    }
    
    private long getMinPeriodMS(BucketStorageType storageType) {
        return getConfig(storageType).getMinPeriod() * 1000;
    }
    
    private long getDefaultMinPeriodMS() {
        return getDefaultConfig().getMinPeriod() * 1000;
    }

    @Test
    // test whether all entries are inserted in the correct order with no overlap between 
    //  buckets
    public void testPut() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries = new TreeMap<AckIdV3, StoredEntry<AckIdV3>>();
        int numMsg = random.nextInt(MAX_MSG);

        enqueueMessages(bm, numMsg, entries);

        assertEquals( numMsg, bm.getCountAfter(null).getEntryCount() );
        
        BucketIterator itr;
        BucketManager.GetNextBucketIteratorResult nbiResult = bm.getNextBucketIterator(null);
        assertFalse( nbiResult.isReachedEnd() );
        itr = nbiResult.getIterator();
        assertNotNull(itr);
        
        AckIdV3 lastEntryId = null;
        boolean firstInBucket = true;
        for( Map.Entry< AckIdV3, StoredEntry< AckIdV3 > > recordedEntry : entries.entrySet() ) {
            StoredEntry<AckIdV3> bucketEntry = itr.next();
            if( bucketEntry == null ) {
                assertNotNull( "First itr pointed to an empty bucket", lastEntryId );
                assert lastEntryId != null; // For findbugs
                
                nbiResult = bm.getNextBucketIterator( itr );
                assertFalse( "Reached end early", nbiResult.isReachedEnd() );
                assertNotNull( nbiResult.getIterator() );
                itr = nbiResult.getIterator();
                assertTrue( "Previous bucket ended after current bucket id", lastEntryId.compareTo( itr.getBucketId() ) < 0 );
                bucketEntry = itr.next();
                assertNotNull( "Empty bucket created", bucketEntry );
                firstInBucket = true;
            }
            
            if( firstInBucket ) {
                Bucket bucket = bm.getBucket( itr.getBucketId() );
                long expectedInterval = getMinPeriodMS(bucket.getBucketStorageType());
                assertTrue( 
                        "First entry created too far from start of bucket", 
                        bucketEntry.getAckId().getTime() - itr.getBucketId().getTime() < expectedInterval );
                firstInBucket = false;
            }
            
            assertEquals( recordedEntry.getKey(), bucketEntry.getAckId() );
            assertTrue( itr.getBucketId().compareTo( bucketEntry.getAckId() ) <= 0 );

            assertArrayEquals( recordedEntry.getValue().getPayload(), bucketEntry.getPayload() );
            lastEntryId = recordedEntry.getKey();
        }
        
        assertNull( "Message left over in the bucket", itr.next() );
        assertTrue( "Messages left over in the bucket manager", bm.getNextBucketIterator( itr ).isReachedEnd() );
        
        shutdownBucketManager(bm);
    }

    /**
     * Validate that all entries in entries appear immediately after the current location of buckerJumper.
     */
    private void validateEntriesMatch(NavigableMap<AckIdV3, StoredEntry<AckIdV3>> entries,
                                      BucketJumper bucketJumper )
            throws SeqStoreException
    {
        for( Map.Entry< AckIdV3, StoredEntry< AckIdV3 > > recordedEntry : entries.entrySet() ) {
            StoredEntry<AckIdV3> storedEntry = bucketJumper.next();
            assertEquals( recordedEntry.getKey(), storedEntry.getAckId() );
            assertArrayEquals( recordedEntry.getValue().getPayload(), storedEntry.getPayload() );
        }
    }

    @Test
    public void testAdvanceTo() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries = new TreeMap<AckIdV3, StoredEntry<AckIdV3>>();
        int numMsg = random.nextInt(MAX_MSG);

        enqueueMessages(bm, numMsg, entries);

        int testTime = getRandomDequeueTime();
        AckIdV3 testId = ackIdGen.getAckId(testTime);
        BucketJumper jumper = new BucketJumper(bm);
        AckIdV3 dest = jumper.advanceTo(testId);
        if (numMsg == 0) {
            assertTrue(dest == null);
        } else {
            assertTrue(dest.compareTo(testId) <= 0);
        }
        
        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > subMap = 
            entries.tailMap( testId, false );
        validateEntriesMatch( subMap, jumper );
        assertNull( jumper.next() );

        shutdownBucketManager(bm);
    }

    private int getRandomDequeueTime() {
        return random.nextInt(MAX_DEQ_TIME - 1) + 1;
    }

    @Test
    public void testAdvanceToWithMoreInserting() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries = new TreeMap<AckIdV3, StoredEntry<AckIdV3>>();
        int numMsg = random.nextInt(MAX_MSG);

        AckIdV3 minEnqueueLevel = ackIdGen.getAckId(1);
        enqueueMessages(bm, minEnqueueLevel, numMsg, entries);

        int testTime = getRandomDequeueTime();
        AckIdV3 testId = ackIdGen.getAckId(testTime);
        BucketJumper jumper = new BucketJumper(bm);
        // test if more messages are inserted, the jumper is still fine
        for (int i = 0; i < numMsg; i++) {
            int dequeuetime = getRandomDequeueTime();
            AckIdV3 id = ackIdGen.getAckId(dequeuetime);
            StoredEntryV3 entry = new StoredEntryV3( id, getStringEntry("message - " + i) );
            bm.insert( id, entry, minEnqueueLevel, true, null );
            entries.put( id, entry );
        }
        testTime = testTime + random.nextInt(MAX_DEQ_TIME - testTime + 1);
        testId = ackIdGen.getAckId(testTime);
        AckIdV3 dest = jumper.advanceTo(testId);
        if (numMsg == 0) {
            assertTrue(dest == null);
        } else {
            assertTrue(dest.compareTo(testId) <= 0);
        }
        
        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > subMap = 
            entries.tailMap( testId, false );
        validateEntriesMatch( subMap, jumper );
        assertNull( jumper.next() );

        shutdownBucketManager(bm);
    }

    @Test
    public void testAdvanceWhenNothingInBM() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        BucketJumper jumper = new BucketJumper(bm);
        AckIdV3 dest = jumper.advanceTo(ackIdGen.getAckId(10000));
        assertTrue(dest == null);
        shutdownBucketManager(bm);
    }

    @Test
    public void testDeleteAllBuckets() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries = new TreeMap<AckIdV3, StoredEntry<AckIdV3>>();
        int numMsg = random.nextInt(MAX_MSG);

        enqueueMessages(bm, numMsg, entries);
        
        BucketJumper jumper = new BucketJumper(bm);
        
        if( numMsg > 1 ) {
            int newPos = random.nextInt( numMsg / 2 ) + ( numMsg / 4 );
            Iterator< AckIdV3 > recordedItr = entries.keySet().iterator();
            for( int cnt = 0; cnt < newPos; ++cnt ) {
                StoredEntry<AckIdV3> stored = jumper.next();
                assertNotNull( stored );
                assertTrue( "Bug in unit test", recordedItr.hasNext() );
                assertEquals( recordedItr.next(), stored.getAckId() );
            }
        }

        AckIdV3 cleanLevel = new AckIdV3( entries.lastKey(), true );
        AckIdV3 lastAvailable = entries.lastKey();
        long lastBucketMinEndTime = 
                ( ( cleanLevel.getTime() / getDefaultMinPeriodMS() ) * getDefaultMinPeriodMS() ) + getDefaultMinPeriodMS();
        
        AckIdV3 minEnqueueLevel = ackIdGen.getAckId( lastBucketMinEndTime ); 
        bm.deleteUpTo(cleanLevel, lastAvailable, minEnqueueLevel);
        assertEquals(cleanLevel, bm.getCleanedTillLevel());
        assertEquals( new StoredCount( 0, 0, 0 ) , bm.getCountAfter( null ) );
        assertNull( jumper.next() );
        
        jumper = new BucketJumper(bm);
        assertNull( jumper.next() );
        
        shutdownBucketManager(bm);
    }

    @Test
    // test when one bucket is there, and the first id is greater than the
    // destination
    public void testAdvanceWhenOneBucket1() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

        AckIdV3 enqueueLevel = ackIdGen.getAckId(9999);
        generateAndInsertMsg(bm, enqueueLevel, 10000);
        BucketJumper jumper = new BucketJumper(bm);
        AckIdV3 dest = jumper.advanceTo(ackIdGen.getAckId(6));
        assertTrue(dest == null);
        shutdownBucketManager(bm);
    }

    @Test
    public void testAdvanceWhenOneBucket2() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

        AckIdV3 enqueueLevel = ackIdGen.getAckId(9999);
        AckIdV3 id1 = generateAndInsertMsg(bm, enqueueLevel, 10000);
        BucketJumper jumper = new BucketJumper(bm);
        AckIdV3 dest = jumper.advanceTo(ackIdGen.next(6));
        assertTrue(dest == null);

        // step 2, can advance to the message
        AckIdV3 id2 = jumper.advanceTo(ackIdGen.next(10000));
        assertTrue(id1.compareTo(id2) == 0);
        shutdownBucketManager(bm);
    }

    @Test
    public void testAdvanceWhenOneBucket3() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

        AckIdV3 enqueueLevel = ackIdGen.getAckId(9999);
        AckIdV3 id1 = generateAndInsertMsg(bm, enqueueLevel, 10000);
        BucketJumper jumper = new BucketJumper(bm);
        AckIdV3 dest = jumper.advanceTo(ackIdGen.next(6));
        assertTrue(dest == null);

        // step 2, can go to next message
        StoredEntry<AckIdV3> next = jumper.next();
        assertTrue(next.getAckId().compareTo(id1) == 0);
        shutdownBucketManager(bm);
    }

    @Test
    // test for 2 bucket, the given time is belong to 2nd bucket, but nothing in 2nd
    // bucket is available yet
    public void testAdvanceWhenTwoBucket() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

        AckIdV3 enqueueLevel = ackIdGen.getAckId(1006);
        generateAndInsertMsg(bm, enqueueLevel, 1006);
        AckIdV3 id2 = generateAndInsertMsg(bm, enqueueLevel, 1007);
        generateAndInsertMsg(bm, enqueueLevel, getDefaultBucketSeperation() + 890);
        BucketJumper jumper = new BucketJumper(bm);
        AckIdV3 dest = jumper.advanceTo(ackIdGen.next(getDefaultBucketSeperation()));
        assertTrue(dest.compareTo(id2) == 0);
        shutdownBucketManager(bm);
    }
    
    @Test
    public void testAdvanceToJustBeforeSecondBucket() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

        AckIdV3 enqueueLevel = ackIdGen.getAckId(1006);
        generateAndInsertMsg(bm, enqueueLevel, 1006);
        AckIdV3 id2 = generateAndInsertMsg(bm, enqueueLevel, 1007);
        AckIdV3 id3 = generateAndInsertMsg(bm, enqueueLevel, getDefaultBucketSeperation() + 890);
        BucketJumper jumper = new BucketJumper(bm);
        AckIdV3 dest = jumper.advanceTo(new AckIdV3( id3, false, 1L ));
        assertTrue(dest.compareTo(id2) == 0);
        assertEquals( 1, bm.getCountAfter(jumper.getPosition() ).getEntryCount() );
        shutdownBucketManager(bm);
    }

    @Test
    // test lastavailable
    public void testLastAvailable() throws SeqStoreException, IOException, InterruptedException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        
        SettableClock settableClock = new SettableClock();
        StoreSpecificClock clock = new StoreSpecificClock( settableClock );

        LastAvailable lastAvailable = new LastAvailable(new StoreIdImpl("test"), bm,
                new DefaultAckIdSourceFactory( ackIdGen, clock ), clock );

        assertNull( lastAvailable.getLastAvailableAckId() );
        assertNull( lastAvailable.getPosition() );
        assertEquals( Long.MAX_VALUE, lastAvailable.getTimeOfNextMessage(null) );
        
        // nothing is in lastAvailable should point to null
        lastAvailable.updateAvailable();
        
        assertNull( lastAvailable.getLastAvailableAckId() );
        assertNull( lastAvailable.getPosition() );
        assertEquals( Long.MAX_VALUE, lastAvailable.getTimeOfNextMessage(null) );
        
        Entry entry = new TestEntry(TEST_PAYLOAD.getBytes(), 50, TEST_PAYLOAD);
        AckIdV3 ackId = lastAvailable.requestAckId(entry.getAvailableTime());
        try {
            bm.insert(
                    ackId, new StoredEntryV3(ackId, entry),
                    lastAvailable.getMinEnqueueLevel(), true, null);
        } finally {
            lastAvailable.enqueueFinished(ackId);
        }
        
        // Shouldn't have moved without a call to updateAvailable
        assertNull( lastAvailable.getLastAvailableAckId() );
        assertNull( lastAvailable.getPosition() );
        
        assertEquals( 50, lastAvailable.getTimeOfNextMessage(null) );
        
        // Shouldn't have changed after calling getTimeOfNextMessage
        assertNull( lastAvailable.getLastAvailableAckId() );
        assertNull( lastAvailable.getPosition() );
        
        lastAvailable.updateAvailable();
        
        // Last available shouldn't be able to move
        assertNull( lastAvailable.getLastAvailableAckId() );
        assertNull( lastAvailable.getPosition() );
        
        settableClock.setCurrentTime(50);
        
        lastAvailable.updateAvailable();
        // Should have advanced
        assertEquals( ackId, lastAvailable.getLastAvailableAckId() );
        assertNotNull( lastAvailable.getPosition() );
        assertEquals( 1, lastAvailable.getPosition().getPosition() );
        shutdownBucketManager(bm);
    }

    @Test
    public void testDeletion() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        
        try {
            long bucketSeperation = getDefaultBucketSeperation();
    
            BucketJumper jumper = new BucketJumper(bm);
            long baseTime = getLowerBucketStartTime( 11 * bucketSeperation ); 
            AckIdV3 enqueueLevel = ackIdGen.getAckId(baseTime);
            AckIdV3 id1 = generateAndInsertMsg(bm, enqueueLevel, baseTime + 6);
            AckIdV3 id2 = generateAndInsertMsg(bm, enqueueLevel, baseTime + 7 );
            long secondBucketStart = baseTime + 2 * bucketSeperation;
            AckIdV3 id3 = generateAndInsertMsg(bm, enqueueLevel, secondBucketStart + 30);
            AckIdV3 id4 = generateAndInsertMsg(bm, enqueueLevel, secondBucketStart + 40);
            // Create a new bucket
            long thirdBucketStart = baseTime + 4 * bucketSeperation;
            AckIdV3 id5 = generateAndInsertMsg(bm, enqueueLevel, thirdBucketStart + 3); 
            AckIdV3 id6 = generateAndInsertMsg(bm, enqueueLevel, thirdBucketStart + 40);
            
            assertEquals( 3, bm.getNumBuckets() );
            
            int count;
            
            // Still in the first bucket
            count = bm.deleteUpTo( ackIdGen.next( baseTime + 6 ), id2, ackIdGen.next( baseTime + 50 ) );
            assertEquals(0, count );
            assertEquals(id1, jumper.next().getAckId());
            
            // After the first bucket but within the min enqueue level still in the min period for the bucket
            count = bm.deleteUpTo( 
                    new AckIdV3( id2, true ), id2, 
                    ackIdGen.next( baseTime + getDefaultMinPeriodMS() - 1 ) );
            assertEquals(0, count);
            
            // After the first bucket and with min enqueue level outside the minimum bucket period
            count = bm.deleteUpTo( 
                    new AckIdV3( id2, true ), id4,
                    ackIdGen.next( baseTime + getDefaultMinPeriodMS() ) );
            
            assertEquals( 2, count );
            // The jumper never saw the second entry because that was deleted before it reached it
            assertEquals(id3, jumper.next().getAckId());
            assertEquals(id4, jumper.next().getAckId());
            
            // In the second bucket but with min enqueue level after the bucket
            count = bm.deleteUpTo( new AckIdV3( id3, true ), id4, ackIdGen.next( thirdBucketStart + 10 ) );
            assertEquals( 0, count );
            
            // After the second bucket and with min enqueue level after the bucket
            count = bm.deleteUpTo( new AckIdV3( id4, true ), id4, ackIdGen.next( thirdBucketStart + 10 ) );
            assertEquals( 2, count );
            
            // Jumper was on the last entry in the deleted bucket and moved to the new bucket
            assertEquals(id5, jumper.next().getAckId());
            
            // Delete the last bucket
            count = bm.deleteUpTo( 
                    new AckIdV3( id6, true ), id6, 
                    ackIdGen.next( thirdBucketStart + getDefaultMinPeriodMS() ) );
            assertEquals( 2, count );
            
            // Jumper skips deleted entry and finds nothing left
            assertEquals( null, jumper.next() );
        } finally {
            shutdownBucketManager(bm);
        }
    }
    
    /**
     * Get the default separation to use between bucket starts. This is a multiple of the
     * bucket period so its easy to know where buckets start.
     * 
     * @return
     */
    private long getDefaultBucketSeperation() {
        BucketStoreConfig config = getDefaultConfig();
        return getBucketSeperation(config);
    }

    private long getBucketSeperation(BucketStoreConfig config) {
        if( config.getMinPeriod() > config.getMaxEnqueueWindow() ) {
            return config.getMinPeriod() * 1000l;
        } else if( config.getMaxEnqueueWindow() % config.getMinPeriod() == 0 ) {
            // The max enqueue window is a multiple of min period so just use it
            return config.getMaxEnqueueWindow() * 1000l;
        } else {
            return ( config.getMaxEnqueueWindow() / config.getMinPeriod() + 1 ) * config.getMinPeriod() * 1000l;  
        }
    }
    
    private long getLowerBucketStartTime( long time ) {
        return ( time / getDefaultMinPeriodMS() ) * getDefaultMinPeriodMS(); 
    }
    
    @Test
    public void testDeleteUnneededBuckets() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        
        long startTime = getDefaultMinPeriodMS() * 11;
        long gapBetweenBuckets = getDefaultBucketSeperation();
        AckIdV3 minEnqueueLevel = new AckIdV3(startTime, false);
        
        List< List< AckIdV3> > bucketMessages = new ArrayList<List<AckIdV3>>();
        for( int i = 0; i < 5; i++ ) {
            long bucketStartTime = startTime + i * gapBetweenBuckets;
            List<AckIdV3> currentBucketMessages = new ArrayList<AckIdV3>(3);
            currentBucketMessages.add( generateAndInsertMsg( bm, minEnqueueLevel, bucketStartTime + 6 ) );
            currentBucketMessages.add( generateAndInsertMsg( bm, minEnqueueLevel, bucketStartTime + 7 ) );
            currentBucketMessages.add( generateAndInsertMsg( bm, minEnqueueLevel, bucketStartTime + 8 ) );
            bucketMessages.add( currentBucketMessages );
        }

        assertEquals(new StoredCount(15, 15 * TEST_ENTRY_SIZE, 5 ), bm.getCountAfter(null) );
        
        NavigableSet<AckIdV3> requiredMessages = 
            new BinarySearchSet<AckIdV3>(
                    Arrays.asList( bucketMessages.get(0).get(1), 
                                   bucketMessages.get(0).get(2), 
                                   bucketMessages.get(2).get(2),
                                   bucketMessages.get(3).get(0) ) );
        
        AckIdV3 maxDeleteLevel = new AckIdV3( bucketMessages.get(4).get(1), true);
        
        minEnqueueLevel = ackIdGen.next(startTime + 4 * gapBetweenBuckets + 10 );
        
        // This should delete the second bucket as no messages in requiredMessages are from it. The 
        // fifth bucket should not be deleted as the max delete level is still within the fifth bucket
        bm.deleteUnneededBuckets(
                maxDeleteLevel, requiredMessages, bucketMessages.get(4).get(2), minEnqueueLevel );
        
        assertEquals(new StoredCount(12, 12 * TEST_ENTRY_SIZE, 4 ), bm.getCountAfter(null) );
        
        BucketJumper jumper = new BucketJumper(bm);
        assertEquals(new StoredCount(12, 12 * TEST_ENTRY_SIZE, 4 ), bm.getCountAfter( jumper.getPosition() ) );
        
        validateIdsmatch( bucketMessages.get(0), jumper );
        assertEquals(new StoredCount(9, 9 * TEST_ENTRY_SIZE, 3 ), bm.getCountAfter( jumper.getPosition() ) );
        
        validateIdsmatch( bucketMessages.get(2), jumper );
        assertEquals(new StoredCount(6, 6 * TEST_ENTRY_SIZE, 2 ), bm.getCountAfter( jumper.getPosition() ) );
        
        validateIdsmatch( bucketMessages.get(3), jumper );
        assertEquals(new StoredCount(3, 3 * TEST_ENTRY_SIZE, 1 ), bm.getCountAfter( jumper.getPosition() ) );
        
        validateIdsmatch( bucketMessages.get(4), jumper );
        assertEquals(new StoredCount(0, 0, 0), bm.getCountAfter( jumper.getPosition() ) );
        
        maxDeleteLevel = new AckIdV3( bucketMessages.get(4).get(2), true);
        minEnqueueLevel = ackIdGen.next(startTime + 4 * gapBetweenBuckets + 10 );
        
        // This should do nothing as even though the maxDelete level is high enough that there
        //  is no need to keep the fifth bucket around for the existing messages it could
        //  still be used for new enqueues
        bm.deleteUnneededBuckets(
                maxDeleteLevel, requiredMessages, bucketMessages.get(4).get(2), minEnqueueLevel );
        
        assertEquals(new StoredCount(12, 12 * TEST_ENTRY_SIZE, 4 ), bm.getCountAfter(null) );
        
        minEnqueueLevel = ackIdGen.next(startTime + 5 * gapBetweenBuckets + 1);
        
        // Still can't delete the 5th bucket as it contains last available
        bm.deleteUnneededBuckets(
                maxDeleteLevel, requiredMessages, bucketMessages.get(4).get(2), minEnqueueLevel );
        
        assertEquals(new StoredCount(12, 12 * TEST_ENTRY_SIZE, 4 ), bm.getCountAfter(null) );
        
        // Allow last available to move upwards
        AckIdV3 last = generateAndInsertMsg( bm, minEnqueueLevel, startTime + 6 * gapBetweenBuckets + 1 );
        minEnqueueLevel = ackIdGen.next(startTime + 6 * gapBetweenBuckets + 1 );
        
        assertEquals(new StoredCount(13, 13 * TEST_ENTRY_SIZE, 5 ), bm.getCountAfter(null) );
        
        // Now delete the 5th bucket
        bm.deleteUnneededBuckets( maxDeleteLevel, requiredMessages, last, minEnqueueLevel );
        
        assertEquals(new StoredCount(10, 10 * TEST_ENTRY_SIZE, 4 ), bm.getCountAfter(null) );
        
        jumper = new BucketJumper(bm);
        assertEquals(new StoredCount(10, 10 * TEST_ENTRY_SIZE, 4 ), bm.getCountAfter( jumper.getPosition() ) );
        
        validateIdsmatch( bucketMessages.get(0), jumper );
        assertEquals(new StoredCount(7, 7 * TEST_ENTRY_SIZE, 3 ), bm.getCountAfter( jumper.getPosition() ) );
        
        validateIdsmatch( bucketMessages.get(2), jumper );
        assertEquals(new StoredCount(4, 4 * TEST_ENTRY_SIZE, 2 ), bm.getCountAfter( jumper.getPosition() ) );
        
        validateIdsmatch( bucketMessages.get(3), jumper );
        assertEquals(new StoredCount( 1, 1 * TEST_ENTRY_SIZE, 1 ), bm.getCountAfter( jumper.getPosition() ) );
        
        StoredEntry<AckIdV3>  lastEntry = jumper.next();
        assertNotNull( lastEntry );
        assertEquals( last, lastEntry.getAckId() );
        
        assertNull( jumper.next() );
        
        shutdownBucketManager(bm);
    }
    
    private void validateIdsmatch( List<AckIdV3> expectedMessages, BucketJumper jumper ) 
        throws SeqStoreException 
    {
        for( AckIdV3 msg : expectedMessages ) {
            StoredEntry<AckIdV3> entry = jumper.next();
            assertNotNull( entry );
            assertEquals( msg, entry.getAckId() );
        }
    }
    
    @Test
    public void testCloseUneededBucketes() throws IOException, SeqStoreException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        
        final int firstDequeueTime = 1006;
        AckIdV3 minEnqueueLevel = ackIdGen.getAckId(firstDequeueTime) ;
        List<AckIdV3> ackIds = new ArrayList<AckIdV3>();
        
        long bucketSeperation = getDefaultBucketSeperation();
        
        ackIds.add( generateAndInsertMsg(bm, minEnqueueLevel, firstDequeueTime) );
        ackIds.add( generateAndInsertMsg(bm, minEnqueueLevel, firstDequeueTime + 3 * bucketSeperation ) );
        ackIds.add( generateAndInsertMsg(bm, minEnqueueLevel, firstDequeueTime + 6 * bucketSeperation ) );
        ackIds.add( generateAndInsertMsg(bm, minEnqueueLevel, firstDequeueTime + 9 * bucketSeperation ) );
        ackIds.add( generateAndInsertMsg(bm, minEnqueueLevel, firstDequeueTime + 12 * bucketSeperation ) );
        
        assertEquals( 5, bm.getNumBuckets() );
        assertEquals( 5, getPersistenceManager().getNumOpenBuckets() );
        
        bm.closeUnusedBucketStores( new AckIdV3( firstDequeueTime + 9 * bucketSeperation + 1, true ) );
        
        // The second and third bucket stores should have been closed
        assertEquals( 3, getPersistenceManager().getNumOpenBuckets() );
        
        // Verify we can still reach everything
        BucketJumper jumper = new BucketJumper(bm);
        for( AckIdV3 ackId : ackIds ) {
            StoredEntry<AckIdV3> entry = jumper.next();
            assertNotNull( entry );
            assertEquals( ackId, entry.getAckId() );
        }
        jumper.close();
        
        assertEquals( 5, getPersistenceManager().getNumOpenBuckets() );
        
        // Verify buckets with an iterator and the next bucket aren't closed
        jumper = new BucketJumper(bm);
        jumper.next();
        jumper.next();
        jumper.next(); // stop on the third bucket
        
        // Should close the second bucket
        bm.closeUnusedBucketStores( new AckIdV3( firstDequeueTime + 12 * bucketSeperation + 1, true ) );
        
        assertEquals( 4, getPersistenceManager().getNumOpenBuckets() );
    }
    
    @Test
    public void testGetStorePosition()  throws IOException, SeqStoreException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        
        long gapBetweenBuckets = getDefaultBucketSeperation();
        
        long firstBucketStart = 11 * gapBetweenBuckets;
        
        AckIdV3 enqueueLevel = ackIdGen.getAckId(firstBucketStart);
        /// Add elements to the bucketmanager. Note that later code assumes two entries per bucket
        NavigableSet<AckIdV3> ackIds = new TreeSet<AckIdV3>();
        // Bucket 1
        ackIds.add( generateAndInsertMsg(bm, enqueueLevel, firstBucketStart + 2) );
        ackIds.add( generateAndInsertMsg(bm, enqueueLevel, firstBucketStart + 3) );
        
        // Bucket 2
        long secondBucketStart = firstBucketStart + gapBetweenBuckets;
        ackIds.add( generateAndInsertMsg(bm, enqueueLevel, secondBucketStart + 1 ) );
        ackIds.add( generateAndInsertMsg(bm, enqueueLevel, secondBucketStart + 3 ) );
        
        // Bucket 3
        long thirdBucketStart = firstBucketStart + gapBetweenBuckets * 2;
        ackIds.add( generateAndInsertMsg(bm, enqueueLevel, thirdBucketStart + 2  ) );
        ackIds.add( generateAndInsertMsg(bm, enqueueLevel, thirdBucketStart + 3 ) );

        assertEquals( 3, bm.getNumBuckets() );
        
        NavigableSet<Long> bucketStartTimes = new TreeSet<Long>();
        bucketStartTimes.addAll( 
                Arrays.asList( new Long[] { firstBucketStart, secondBucketStart, thirdBucketStart } ) );
        
        NavigableSet<AckIdV3> ackIdsToTest = new TreeSet<AckIdV3>();
        // Before any buckets
        ackIdsToTest.add( new AckIdV3(  900, false  ) );
        // In first bucket before anything
        ackIdsToTest.add( new AckIdV3( firstBucketStart + 1, false ) );
        // In first bucket but after everything
        ackIdsToTest.add( new AckIdV3( firstBucketStart + 4, false ) );
        // In second bucket but before everything
        ackIdsToTest.add( new AckIdV3( secondBucketStart, false ) );
        // In second bucket but after everything
        ackIdsToTest.add( new AckIdV3( secondBucketStart + 5, false ) );
        
        BucketStoreConfig config = getDefaultConfig();
        if( config.getMaxEnqueueWindow() > config.getMinPeriod() ) {
            long maxExtensionTime = config.getMaxEnqueueWindow() * 1000;
            // In second bucket but within the extension window
            ackIdsToTest.add( new AckIdV3( secondBucketStart + maxExtensionTime - 1, false ) );
            
            // After the second bucket but before third bucket
            ackIdsToTest.add( new AckIdV3( secondBucketStart + maxExtensionTime + 1, false ) );
        } else {
            // After the second bucket but before third bucket
            ackIdsToTest.add( new AckIdV3( secondBucketStart + config.getMinPeriod() * 1000 + 10, false ) );
        }
        
        // In last bucket but after last id
        ackIdsToTest.add( new AckIdV3( thirdBucketStart + 5, false ) );
        // After everything
        ackIdsToTest.add( new AckIdV3( firstBucketStart + 10 * gapBetweenBuckets + 5, false ) );
        
        int cnt = 0;
        for( AckIdV3 ackId : ackIds ) {
            ackIdsToTest.add( new AckIdV3( ackId, false ) );
            ackIdsToTest.add( ackId );
            ackIdsToTest.add( new AckIdV3( ackId, true ) );

            // Assumes two entries per bucket
            long bucketPos = (cnt % 2) + 1;
            ackIdsToTest.add( new AckIdV3( ackId, false, bucketPos ) );
            ackIdsToTest.add( new AckIdV3( ackId, null, bucketPos ) );
            ackIdsToTest.add( new AckIdV3( ackId, true, bucketPos ) );
            cnt++;
        }
        
        AckIdV3 previousBucketId = null;
        for( AckIdV3 ackId : ackIdsToTest ) {
            StorePosition pos = bm.getStorePosition( ackId );
            if( pos == null ) {
                assertTrue( ackIds.headSet(ackId).isEmpty() );
                assertNull( previousBucketId );
                assertNull( bucketStartTimes.floor(ackId.getTime() ) );
            } else {
                AckIdV3 bucketId = pos.getBucketId();
                assertTrue( bucketId.compareTo( ackId ) <= 0 );
                
                Long lowerBucketStart = bucketStartTimes.floor(ackId.getTime());
                assertEquals( "Wrong bucket returned for AckId " + ackId + ": bucketId = " + bucketId,
                              lowerBucketStart.longValue(), bucketId.getTime() );
                
                previousBucketId = bucketId;
                
                assertEquals( ackIds.subSet( bucketId, true, ackId, true ).size(), pos.getPosition() );
                
                if( pos.getKey() != null ) {
                    assertEquals( pos.getKey().getBucketPosition().longValue(), pos.getPosition() );
                    assertEquals( ackIds.floor( ackId ), pos.getKey() );
                } else {
                    assertEquals( 0, pos.getPosition() );
                }
                
            }
        }
        
        shutdownBucketManager(bm);
    }
    
    @Test
    public void testGetCountAfter() throws IOException, SeqStoreException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        
        long gapBetweenBuckets = getDefaultBucketSeperation();
        long firstBucketTime = gapBetweenBuckets * 8;
        long firstDequeueTime = firstBucketTime + 6;
        
        AckIdV3 minEnqueueLevel = ackIdGen.next(firstBucketTime);
        
        NavigableSet<AckIdV3> messages = new TreeSet<AckIdV3>();
        messages.add( generateAndInsertMsg(bm, minEnqueueLevel, firstDequeueTime ) );
        assertEquals( new StoredCount( 1, TEST_ENTRY_SIZE, 1 ), bm.getCountAfter( null ) );
        
        for( int cnt = 0; cnt < 100; ++cnt ) {
            messages.add( generateAndInsertMsg(bm, minEnqueueLevel, firstDequeueTime + 2 ) );
        }
        
        assertEquals( new StoredCount( 101, 101 * TEST_ENTRY_SIZE, 1 ), bm.getCountAfter( null ) );
        
        // Start a new bucket
        messages.add( generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + gapBetweenBuckets + 1 ) );
        
        assertEquals( new StoredCount( 102, 102 * TEST_ENTRY_SIZE, 2 ), bm.getCountAfter( null ) );
        
        for( int cnt = 0; cnt < 50; ++cnt ) {
            messages.add( generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + gapBetweenBuckets + 1) );
        }
        
        assertEquals( new StoredCount( 152, 152 * TEST_ENTRY_SIZE, 2 ), bm.getCountAfter( null ) );
        
        AckIdV3 minEnqueueTime = ackIdGen.next(firstBucketTime + 2 * gapBetweenBuckets);
        AckIdV3 lastAvailable = messages.floor(minEnqueueTime);
        assertEquals( 0, bm.deleteUpTo( new AckIdV3( firstDequeueTime - 1, true ), lastAvailable, minEnqueueTime ) );
        
        assertEquals( new StoredCount( 152, 152 * TEST_ENTRY_SIZE, 2 ), bm.getCountAfter( null ) );
        
        assertEquals( 0, bm.deleteUpTo( new AckIdV3( firstDequeueTime + 1, true ), lastAvailable, minEnqueueTime ) );
        
        assertEquals( new StoredCount( 152, 152 * TEST_ENTRY_SIZE, 2 ), bm.getCountAfter( null ) );
        
        assertEquals( 101, bm.deleteUpTo( new AckIdV3( firstDequeueTime + 3, true ), lastAvailable, minEnqueueTime ) );
        
        assertEquals( new StoredCount( 51, 51 * TEST_ENTRY_SIZE, 1 ), bm.getCountAfter( null ) );
        
        assertEquals( 
                51, bm.deleteUpTo( new AckIdV3( firstBucketTime + gapBetweenBuckets + 2, true ), lastAvailable, minEnqueueTime ) );
        
        assertEquals( new StoredCount( 0, 0, 0 ), bm.getCountAfter( null ) );
    }
    
    private void assertMessageCountAndSizeMatches(long expectedMessageCount, long expectedSize, StoredCount actual) {
        assertNotNull( actual );
        assertEquals( expectedMessageCount, actual.getEntryCount() );
        assertEquals( expectedSize, actual.getRetainedBytesCount() );
    }

    @Test
    public void testGetCount() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        
        long gapBetweenBuckets = getDefaultBucketSeperation();
        long firstBucketTime = 5 * gapBetweenBuckets;
        AckIdV3 minEnqueueLevel = ackIdGen.next(firstBucketTime);
        
        BucketJumper from = new BucketJumper(bm);
        BucketJumper to = new BucketJumper(bm);
        generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + 6);
        assertEquals( new StoredCount( 0, 0, 0 ), bm.getCount(from.getPosition(), to.getPosition() ) );
        from.next();
        to.next();
        generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + 7);
        generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + 8);
        generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + 9);
        AckIdV3 id2 = generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + 10);
        to.next();
        assertEquals( new StoredCount( 1 , 5 * TEST_ENTRY_SIZE, 1 ), bm.getCount(from.getPosition(), to.getPosition() ) );
        // test one bucket from and to point to different thing
        to.advanceTo(id2);
        assertEquals( new StoredCount( 4 , 5 * TEST_ENTRY_SIZE, 1 ), bm.getCount(from.getPosition(), to.getPosition() ) );
        // test two bucket
        AckIdV3 id3 = generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + gapBetweenBuckets + 9);
        to.advanceTo(id3);
        assertEquals( new StoredCount( 5 , 6 * TEST_ENTRY_SIZE, 2 ), bm.getCount(from.getPosition(), to.getPosition() ) );
        id3 = generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + gapBetweenBuckets + 301);
        to.advanceTo(id3);
        assertEquals( new StoredCount( 6 , 7 * TEST_ENTRY_SIZE, 2 ), bm.getCount(from.getPosition(), to.getPosition() ) );
        // test multiple buckets
        long lastEntryTime = firstBucketTime + 5 * gapBetweenBuckets;
        createAndEnqueueMsgs(bm, minEnqueueLevel, 10, firstBucketTime + gapBetweenBuckets, lastEntryTime);
        to.advanceTo(ackIdGen.next(lastEntryTime));
        assertMessageCountAndSizeMatches( 16 , 17 * TEST_ENTRY_SIZE, bm.getCount(from.getPosition(), to.getPosition() ) );
        to.advanceTo(ackIdGen.next(lastEntryTime + 5 * gapBetweenBuckets ));
        assertEquals( new StoredCount( 0, 0, 0 ), bm.getCountAfter( to.getPosition() ) );
        assertMessageCountAndSizeMatches( 16 , 17 * TEST_ENTRY_SIZE, bm.getCount(from.getPosition(), to.getPosition() ) );
        // test getCountAfter
        assertMessageCountAndSizeMatches( 16 , 17 * TEST_ENTRY_SIZE, bm.getCountAfter( from.getPosition() ) );
        from.next();
        assertMessageCountAndSizeMatches( 15 , 17 * TEST_ENTRY_SIZE, bm.getCountAfter( from.getPosition() ) );
        from.advanceTo(ackIdGen.next(firstBucketTime+10));
        assertMessageCountAndSizeMatches( 12 , 12 * TEST_ENTRY_SIZE, bm.getCountAfter( from.getPosition() ) );
        from.advanceTo(ackIdGen.next(lastEntryTime));
        assertEquals( new StoredCount( 0, 0, 0 ), bm.getCountAfter( from.getPosition() ) );
        shutdownBucketManager(bm);
    }

    /*void insertEntry( BucketManager bm, AckIdV3 minEnqueueLevel, AckIdV3 id ) throws SeqStoreException {
        StoredEntryV3 entry = new StoredEntryV3(id, getStringEntry("message - " + id ));
        bm.insert( id, entry, minEnqueueLevel, true, null );
    }*/

    @Test
    public void testBucketExtention() throws SeqStoreException, IOException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        
        BucketStoreConfig config = getDefaultConfig();
        long maxEnqueueWinMS = config.getMaxEnqueueWindow() * 1000;
        long maxPeriodMS = config.getMaxPeriod() * 1000;
        long minPeriodMS = config.getMinPeriod() * 1000;
        long minSize = config.getMinimumSize() * 1024;
        
        // We need at least this gap for some of the tests
        assertTrue( "Test config should have a larger maximum bucket period",
                    maxPeriodMS > minPeriodMS + maxEnqueueWinMS );
        
        long firstBucketTime = 17 * minPeriodMS;
        
        AckIdV3 minEnqueueLevel = ackIdGen.next( firstBucketTime );
        generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + 1);
        assertEquals( 1, bm.getNumBuckets() );
        
        if( config.getMaxEnqueueWindow() > config.getMinPeriod() ) {
            // Should extend the first bucket
            generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + maxEnqueueWinMS - 1);
            assertEquals( 1, bm.getNumBuckets() );
        }
        
        // Move min enqueue level to the end of the first bucket
        minEnqueueLevel = ackIdGen.next( firstBucketTime + minPeriodMS );
        
        // Should extend the first bucket
        generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + minPeriodMS + maxEnqueueWinMS - 1);
        assertEquals( 1, bm.getNumBuckets() );
        
        // But this should be too far
        generateAndInsertMsg(bm, minEnqueueLevel, firstBucketTime + minPeriodMS + maxEnqueueWinMS );
        assertEquals( 2, bm.getNumBuckets() );
        
        long secondGroupTime = getLowerBucketStartTime( firstBucketTime + 2 * maxPeriodMS );
        minEnqueueLevel = ackIdGen.next( secondGroupTime + 1 );
        generateAndInsertMsg(bm, minEnqueueLevel, secondGroupTime + 1);
        assertEquals( 3, bm.getNumBuckets() );
        
        long bucketSize = TEST_PAYLOAD.length(); 
        while( bucketSize < minSize ) {
            generateAndInsertMsg(bm, minEnqueueLevel, secondGroupTime + 2);
            bucketSize += TEST_PAYLOAD.length();
        }
        // Even if the size is large than the minimum a new bucket shouldn't be created because the messages
        //  are still in the minimum period
        assertEquals( 3, bm.getNumBuckets() );
        
        // Should still be in the same bucket
        generateAndInsertMsg(bm, minEnqueueLevel, secondGroupTime + minPeriodMS - 1);
        assertEquals( 3, bm.getNumBuckets() );
        
        // Should be a new bucket
        generateAndInsertMsg(bm, minEnqueueLevel, secondGroupTime + minPeriodMS);
        assertEquals( 4, bm.getNumBuckets() );
        
        // Test extending to the maximum time
        long thirdGroupTime = getLowerBucketStartTime( secondGroupTime + 2 * maxPeriodMS );
        minEnqueueLevel = ackIdGen.next( thirdGroupTime + 1 );
        generateAndInsertMsg(bm, minEnqueueLevel, thirdGroupTime + 1);
        assertEquals( 5, bm.getNumBuckets() );
        
        long maxEnqueueTime = thirdGroupTime + maxPeriodMS;
        minEnqueueLevel = ackIdGen.next( maxEnqueueTime - 1 );
        
        // Should still be in the same bucket
        generateAndInsertMsg(bm, minEnqueueLevel, maxEnqueueTime - 1);
        assertEquals( 5, bm.getNumBuckets() );
        
        // And this should finally be too far
        generateAndInsertMsg(bm, minEnqueueLevel, maxEnqueueTime);
        assertEquals( 6, bm.getNumBuckets() );
        
        // Test not extending buckets eligible for deletion
        long fourthGroupTime = getLowerBucketStartTime( thirdGroupTime + 2 * maxPeriodMS );
        minEnqueueLevel = ackIdGen.next( fourthGroupTime + 1 );
        
        AckIdV3 lastId = generateAndInsertMsg(bm, minEnqueueLevel, fourthGroupTime + 1);
        // Delete everything but the most recently enqueued message
        bm.deleteUpTo(minEnqueueLevel, lastId, minEnqueueLevel);
        assertEquals( 1, bm.getNumBuckets() );
        
        // Should reuse the same bucket
        lastId = generateAndInsertMsg(bm, minEnqueueLevel, fourthGroupTime + minPeriodMS - 2 );
        assertEquals( 1, bm.getNumBuckets() );
        
        // Move min enqueue level past the last message but still inside the bucket
        minEnqueueLevel = ackIdGen.next( fourthGroupTime + minPeriodMS - 1 );
        
        // Mark all messages as deleted but the bucket should remain as the enqueue level
        //  is still in the bucket
        bm.deleteUpTo(new AckIdV3(lastId, true), lastId, minEnqueueLevel);
        assertEquals( 1, bm.getNumBuckets() );
        
        minEnqueueLevel = ackIdGen.next( fourthGroupTime + minPeriodMS + 1 );
        
        AckIdV3 secondToLastId = lastId;
        // Should create a new bucket as everything in the old bucket is deletable
        lastId = generateAndInsertMsg(bm, minEnqueueLevel, fourthGroupTime + minPeriodMS + 2 );
        assertEquals( 2, bm.getNumBuckets() );
        
        // Should finally delete the previous bucket
        bm.deleteUpTo(new AckIdV3(secondToLastId, true), lastId, minEnqueueLevel);
        assertEquals( 1, bm.getNumBuckets() );
        
        shutdownBucketManager(bm);
    }
    
    /**
     * Test the transition to dedicated buckets once there is enough data in the store.
     * @throws SeqStoreException 
     * @throws IOException 
     */
    @Test
    public void testTransitionToDedicatedBuckets() throws IOException, SeqStoreException {
        BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);
        if( !getPersistenceManager().getSupportedNewBucketTypes().contains( BucketStorageType.SharedDatabase ) ) {
            shutdownBucketManager(bm);
            // Don't run the test
            Assume.assumeTrue(false);
            return;
        }
        
        List<AckIdV3> expectedMessages = new ArrayList<AckIdV3>(); 
        long sharedBucketGap = getBucketSeperation(SHARED_BUCKET_CONFIGURATION);
        long startTime = sharedBucketGap * 17;
        
        AckIdV3 minEnqueueLevel = ackIdGen.getAckId( startTime );
        expectedMessages.add( generateAndInsertMsg(bm, minEnqueueLevel, startTime) );
        assertEquals( 1, bm.getNumBuckets() );
        assertEquals( 0, bm.getNumDedicatedBuckets() );
        
        long perMessageSize = TEST_PAYLOAD.length() + AckIdV3.LENGTH;
        long storeSize = perMessageSize;
        // Stop just before the transition point
        long limit = TRANSITION_TO_DEDICATED_BUCKET_KB * 1024;
        long timeIncrement = sharedBucketGap / 10; 
        long time = startTime + timeIncrement;
        while( storeSize < limit ) {
            expectedMessages.add( generateAndInsertMsg(bm, minEnqueueLevel, time) );
            time += timeIncrement;
            storeSize += perMessageSize;
        }
        long numBuckets = bm.getNumBuckets();
        assertEquals( 0, bm.getNumDedicatedBuckets() );
        
        // Should create a new dedicated bucket
        expectedMessages.add( generateAndInsertMsg(bm, minEnqueueLevel, time + 2 * sharedBucketGap ) );
        assertEquals( numBuckets + 1, bm.getNumBuckets() );
        assertEquals( 1, bm.getNumDedicatedBuckets() );
        
        // Test that iterating across all the entries works
        BucketJumper jumper = new BucketJumper(bm);
        validateIdsmatch(expectedMessages, jumper );
        assertEquals( null, jumper.next() );
        jumper.close();
        
        shutdownBucketManager(bm);
    }

    @Test
    public void testFallingOffEndOfLastBucket() throws IOException, SeqStoreException {
        final String payload = "PAYLOAD";
        final SettableClock testclock = new SettableClock();
        final BucketManager manager = createBucketManager(testclock, true);
        final BucketJumper jumper = new BucketJumper(manager);

        testclock.setCurrentTime(System.currentTimeMillis());

        AckIdV3 minEnqueueLevel = ackIdGen.next(100);
        insertMsg(manager, minEnqueueLevel, 100, payload);
        insertMsg(manager, minEnqueueLevel, 101, payload);
        
        assertNull( jumper.getPosition() );
        
        StoredEntry<AckIdV3> entry;
        
        entry = jumper.next();
        assertNotNull( entry );
        assertEquals( 100l, entry.getAckId().getTime() );
        assertNotNull( jumper.getPosition() );
        assertEquals( 1l, jumper.getPosition().getPosition() );
        
        entry = jumper.next();
        assertNotNull( entry );
        assertEquals( 101l, entry.getAckId().getTime() );
        StorePosition position = jumper.getPosition();
        assertEquals( 2l, position.getPosition() );
        
        entry = jumper.next();
        assertNull( entry );
        assertEquals( position, jumper.getPosition() );
        
        shutdownBucketManager(manager);
    }
    
    protected void enqueueMessages(BucketManager bm, int numMsg, 
        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries) throws SeqStoreException 
    {
        AckIdV3 minEnqueuelevel = ackIdGen.next(1);
        enqueueMessages( bm, minEnqueuelevel, numMsg, entries );
    }

    protected void enqueueMessages(BucketManager bm,  AckIdV3 minEnqueueLevel, int numMsg, 
                                   NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries) throws SeqStoreException 
    {
        addMessage(0, 1, bm, minEnqueueLevel, entries);

        addMessage(1, MAX_DEQ_TIME - 1, bm, minEnqueueLevel, entries);
        for (int i = 2; i < numMsg; i++) {
            int dequeuetime = getRandomDequeueTime();
            addMessage(i, dequeuetime, bm, minEnqueueLevel, entries);
        }
    }

    private void addMessage(int i, int dequeuetime, BucketManager bm, AckIdV3 minEnqueueLevel,
                            NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries) throws SeqStoreException 
    {
        AckIdV3 id = ackIdGen.next(dequeuetime);
        StoredEntryV3 entry = new StoredEntryV3(id, getStringEntry("message - " + i));
        bm.insert(id, entry, minEnqueueLevel, true, null);
        entries.put( id, entry );
    }

    private AckIdV3 generateAndInsertMsg(BucketManager bm, AckIdV3 minEnqueueLevel, long dequeuetime) throws SeqStoreException {
        return insertMsg(bm, minEnqueueLevel, dequeuetime, TEST_PAYLOAD);
    }

    private AckIdV3 insertMsg(BucketManager bm, AckIdV3 minEnqueueLevel, long dequeuetime, String payload)
            throws SeqStoreException {
        AckIdV3 id = ackIdGen.next(dequeuetime);
        StoredEntryV3 entry = new StoredEntryV3(id, getStringEntry(payload));
        bm.insert(id, entry, minEnqueueLevel, true, null);
        return id;
    }

    private void createAndEnqueueMsgs(BucketManager bm, AckIdV3 minEnqueueLevel, long numMsg, long min, long max)
            throws SeqStoreException {
        for (int i = 0; i < numMsg; i++) {
            long dequeueTime = min + random.nextInt((int) (max - min));
            generateAndInsertMsg(bm, minEnqueueLevel, dequeueTime);
        }
    }

    private TestEntry getStringEntry(String s) {
        return new TestEntry(s.getBytes());
    }
}
