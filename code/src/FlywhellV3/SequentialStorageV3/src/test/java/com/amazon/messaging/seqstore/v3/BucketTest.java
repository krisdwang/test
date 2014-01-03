package com.amazon.messaging.seqstore.v3;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

import lombok.Cleanup;

import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.bdb.BDBPersistentManager;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.mapPersistence.NonPersistentBucketCreator;
import com.amazon.messaging.seqstore.v3.store.Bucket;
import com.amazon.messaging.seqstore.v3.store.BucketImpl;
import com.amazon.messaging.seqstore.v3.store.BucketIterator;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.store.StorePosition;
import com.amazon.messaging.testing.LabelledParameterized;
import com.amazon.messaging.testing.LabelledParameterized.Parameters;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Scheduler;

/**
 * This test tests everything about buckets except iterators which are tested by {@link BucketIteratorTestBase}
 * @author stevenso
 *
 */
@RunWith(LabelledParameterized.class)
public class BucketTest extends TestCase {
    private static final StoreIdImpl STORE_NAME = new StoreIdImpl("BucketTest", "");
    
    private static final AckIdGenerator generator = new AckIdGenerator();
    
    @Parameters
    public static Collection<TestParameters> getStorageTypeTestParameters() {
        return Arrays.asList(
                new TestParameters( "Map", new Object[] { false, BucketStorageType.DedicatedDatabase, false } ),
                new TestParameters( "BDB-LLDedicated-UseLLM", 
                        new Object[] { true, BucketStorageType.DedicatedDatabaseInLLMEnv, true } ),
                new TestParameters( "BDB-LLDedicated-!UseLLM", 
                        new Object[] { true, BucketStorageType.DedicatedDatabaseInLLMEnv, false } ),
                new TestParameters( "BDB-Dedicated-UseLLM", 
                        new Object[] { true, BucketStorageType.DedicatedDatabase, true } ),
                new TestParameters( "BDB-Dedicated-!UseLLM", 
                        new Object[] { true, BucketStorageType.DedicatedDatabase, false } ),
                new TestParameters( "BDB-Shared-UseLLM", 
                        new Object[] { true, BucketStorageType.SharedDatabase, true } ),
                new TestParameters( "BDB-Shared-!UseLLM", 
                        new Object[] { true, BucketStorageType.SharedDatabase, false } ));
    }
    
    private final boolean useBDB;
    
    private final BucketStorageType bucketStorageType;
    
    private final boolean useLLMEnv;
    
    public BucketTest(boolean useBDB, BucketStorageType bucketStorageType, boolean useLLMEnv) {
        this.useBDB = useBDB;
        this.bucketStorageType = bucketStorageType;
        this.useLLMEnv = useLLMEnv;
    }

    protected StorePersistenceManager createPersistenceManager(boolean truncate)
        throws SeqStoreException, IOException
    {
        if( useBDB ) {
            SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
            if( truncate ) {
                con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
                con.setTruncateDatabase(true);
            } else {
                con.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
                con.setTruncateDatabase(false);
            }
            con.setUseSeperateEnvironmentForDedicatedBuckets(useLLMEnv);
            return new BDBPersistentManager( con.getImmutableConfig(), Scheduler.getGlobalInstance() );
        } else {
            return new NonPersistentBucketCreator<BasicInflightInfo>();
        }
    }
    
    private StoredEntry<AckIdV3> insertMessage( Bucket bucket, long time, int id ) 
            throws SeqStoreDatabaseException 
    {
        AckIdV3 ackId = generator.getAckId( time );
        StoredEntry<AckIdV3> entry = new StoredEntryV3( ackId, ( "Message" + id ).getBytes(), "Log" + id );
        bucket.put( ackId, entry, true );
        return entry;
    }
    
    private void insertMessage( Map< AckIdV3, StoredEntry<AckIdV3> > map, Bucket bucket, long time, int id ) 
            throws SeqStoreDatabaseException 
    {
        StoredEntry<AckIdV3> entry = insertMessage( bucket, time, id );
        map.put( entry.getAckId(), entry );
    }
    
    private void assertStoredEntryEquals( StoredEntry<AckIdV3> expected, StoredEntry<AckIdV3> actual ) {
        assertNotNull( actual );
        assertEquals( expected, actual );
        assertArrayEquals( expected.getPayload(), actual.getPayload() );
    }
    
    private Bucket createBucket(StorePersistenceManager manager) throws SeqStoreException {
        AckIdV3 bucketId = new AckIdV3( 1, true );
        return BucketImpl.createNewBucket(STORE_NAME, bucketId, bucketStorageType, manager);
    }
    
    @Test
    public void testEmptyBucket() throws SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager(true);
        Bucket bucket = createBucket(manager);

        assertNull( bucket.getFirstId() );
        assertNull( bucket.getLastId() );
        assertEquals( 0, bucket.getByteCount() );
        assertEquals( 0, bucket.getEntryCount() );
        assertEquals( true, bucket.isCountTrusted() );
        assertEquals( 0, bucket.getAccurateEntryCount() );
        assertNull( bucket.get( generator.getAckId(1 ) ) );
        assertEquals( new StorePosition(bucket.getBucketId(), null), bucket.getPosition(generator.getAckId(1) ) );
    }
    
    @Test
    public void testPutAndGet() throws SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager(true);
        Bucket bucket = createBucket(manager);
        
        StoredEntry<AckIdV3> entry = insertMessage( bucket, 5, 1 );
        assertStoredEntryEquals( entry, bucket.get( entry.getAckId() ) );
        
        AckIdV3 withPosition = new AckIdV3( entry.getAckId(), 1 );
        assertStoredEntryEquals( entry, bucket.get( withPosition ) );
        
        StoredEntry<AckIdV3> entry2 = insertMessage( bucket, 5, 1 );
        assertStoredEntryEquals( entry, bucket.get( entry.getAckId() ) );
        assertStoredEntryEquals( entry2, bucket.get( entry2.getAckId() ) );
        
        // An entry that doesn't exist shouldn't get anything
        assertNull( bucket.get( generator.getAckId( 7 ) ) );
        
        assertEquals( entry.getAckId(), bucket.getFirstId() );
        assertEquals( entry2.getAckId(), bucket.getLastId() );
        assertEquals( entry.getPayloadSize() + entry2.getPayloadSize() + 2 * AckIdV3.LENGTH, bucket.getByteCount() );
        assertEquals( 2, bucket.getEntryCount() );
        assertEquals( true, bucket.isCountTrusted() );
        assertEquals( 2, bucket.getAccurateEntryCount() );
    }
    
    @Test
    public void testCloseBucketStore() throws SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager(true);
        Bucket bucket = createBucket(manager);
        
        StoredEntry<AckIdV3> entry = insertMessage( bucket, 5, 1 );
        StoredEntry<AckIdV3> entry2 = insertMessage( bucket, 5, 1 );
        
        assertEquals( 1, manager.getNumOpenBuckets() );
        bucket.closeBucketStore();
        assertEquals( 0, manager.getNumOpenBuckets() );
        
        // None of these should reopen the bucket
        assertEquals( entry.getAckId(), bucket.getFirstId() );
        assertEquals( entry2.getAckId(), bucket.getLastId() );
        assertEquals( entry.getPayloadSize() + entry2.getPayloadSize() + 2 * AckIdV3.LENGTH, bucket.getByteCount() );
        assertEquals( 2, bucket.getEntryCount() );
        assertEquals( true, bucket.isCountTrusted() );
        assertEquals( 2, bucket.getAccurateEntryCount() );
        
        assertEquals( 0, manager.getNumOpenBuckets() );
        
        // Should be automatically reopened
        assertStoredEntryEquals( entry, bucket.get( entry.getAckId() ) );
        
        assertEquals( 1, manager.getNumOpenBuckets() );
        bucket.closeBucketStore();
        assertEquals( 0, manager.getNumOpenBuckets() );
        
        // Should reopen the bucket
        BucketIterator itr = bucket.getIter();
        assertNotNull( itr );
        assertEquals( 1, manager.getNumOpenBuckets() );
        
        assertStoredEntryEquals( entry, itr.next() );
        
        // Should fail to close the bucket store
        bucket.closeBucketStore();
        assertEquals( 1, manager.getNumOpenBuckets() );
        
        bucket.closeIterator(itr);

        // Should close now that there are no iterators
        bucket.closeBucketStore();
        assertEquals( 0, manager.getNumOpenBuckets() );
    }
    
    @Test
    public void testClose() throws SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager(true);
        Bucket bucket = createBucket(manager);
        
        StoredEntry<AckIdV3> entry = insertMessage( bucket, 5, 1 );
        StoredEntry<AckIdV3> entry2 = insertMessage( bucket, 5, 1 );
        
        assertEquals( 1, manager.getNumOpenBuckets() );
        assertEquals( 1, manager.getNumBuckets() );
        if( bucketStorageType.isDedicated() ) {
            assertEquals( 1, manager.getNumOpenDedicatedBuckets() );
            assertEquals( 1, manager.getNumDedicatedBuckets() );
        } else {
            assertEquals( 0, manager.getNumDedicatedBuckets() );
            assertEquals( 0, manager.getNumOpenDedicatedBuckets() );
        }
        
        BucketIterator itr = bucket.getIter();
        
        bucket.close();
        
        assertEquals( Bucket.BucketState.CLOSED, bucket.getBucketState() );
        assertEquals( 0, manager.getNumOpenBuckets() );
        assertEquals( 0, manager.getNumOpenDedicatedBuckets() );
        assertEquals( 1, manager.getNumBuckets() );
        
        verifyBucketAfterCloseOrDelete( 
                bucket, itr, 2, entry.getPayloadSize() + entry2.getPayloadSize() + 2 * AckIdV3.LENGTH, false);
    }
    
    @Test
    public void testDelete() throws SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager(true);
        Bucket bucket = createBucket(manager);
        
        StoredEntry<AckIdV3> entry = insertMessage( bucket, 5, 1 );
        StoredEntry<AckIdV3> entry2 = insertMessage( bucket, 5, 1 );
        
        assertEquals( 1, manager.getNumOpenBuckets() );
        assertEquals( 1, manager.getNumBuckets() );
        if( bucketStorageType.isDedicated() ) {
            assertEquals( 1, manager.getNumDedicatedBuckets() );
            assertEquals( 1, manager.getNumDedicatedBuckets() );
        } else {
            assertEquals( 0, manager.getNumDedicatedBuckets() );
            assertEquals( 0, manager.getNumDedicatedBuckets() );
        }
        
        BucketIterator itr = bucket.getIter();
        
        bucket.deleted();
        manager.deleteBucketStore(STORE_NAME, bucket.getBucketId());
        
        assertEquals( Bucket.BucketState.DELETED, bucket.getBucketState() );
        assertEquals( 0, manager.getNumOpenBuckets() );
        assertEquals( 0, manager.getNumBuckets() );
        assertEquals( 0, manager.getNumDedicatedBuckets() );
        
        verifyBucketAfterCloseOrDelete( 
                bucket, itr, 2, entry.getPayloadSize() + entry2.getPayloadSize() + 2 * AckIdV3.LENGTH, true);
    }
    
    private void verifyBucketAfterCloseOrDelete(
        Bucket bucket, BucketIterator itr, long expectedEntries, long expectedSize, boolean deleted) 
                throws SeqStoreDatabaseException 
    {
        // All of these should still work
        assertEquals( expectedEntries, bucket.getEntryCount() );
        assertEquals( expectedSize, bucket.getByteCount() );
        assertEquals( true, bucket.isCountTrusted() );
        assertEquals( 
                new BucketPersistentMetaData( 
                        bucket.getBucketStoreSequenceId(),
                        bucket.getBucketId(), bucket.getBucketStorageType(),
                        bucket.getEntryCount(), bucket.getByteCount(),
                        deleted ), 
                bucket.getMetadata() );
        assertEquals( 0, bucket.getNumIterators() );

        // All of these should fail
        try {
            bucket.copyIter(itr);
            fail( "should have thrown IllegalStateException");
        } catch( IllegalStateException e ) {
            // Success
        }
        
        try {
            bucket.get(generator.getAckId( 5 ) );
            fail( "should have thrown IllegalStateException");
        } catch( IllegalStateException e ) {
            // Success
        }
        
        try {
            bucket.getAccurateEntryCount();
            fail( "should have thrown IllegalStateException");
        } catch( IllegalStateException e ) {
            // Success
        }
        
        if( deleted ) {
            assertNull( bucket.getFirstId() );
            assertNull( bucket.getLastId() );
        } else {
            try {
                bucket.getFirstId();
                fail( "should have thrown IllegalStateException");
            } catch( IllegalStateException e ) {
                // Success
            }
            
            try {
                bucket.getLastId();
                fail( "should have thrown IllegalStateException");
            } catch( IllegalStateException e ) {
                // Success
            }
        }
        
        try {
            bucket.getPosition(new AckIdV3(5,true));
            fail( "should have thrown IllegalStateException");
        } catch( IllegalStateException e ) {
            // Success
        }
        
        try {
            insertMessage( bucket, 5, 1 );
            fail( "should have thrown IllegalStateException");
        } catch( IllegalStateException e ) {
            // Success
        }
        
        // Should be ignored
        bucket.closeIterator(itr);
        bucket.close();
        bucket.deleted();
    }
    
    @Test
    public void testReloadEmptyBucket() throws SeqStoreException, IOException {
        if( !useBDB ) {
            return;
        }
        
        StorePersistenceManager manager = createPersistenceManager(true);
        try {
            @Cleanup
            Bucket bucket = createBucket(manager);

            assertNull( bucket.getFirstId() );
            assertNull( bucket.getLastId() );
            assertEquals( 0, bucket.getByteCount() );
            assertEquals( 0, bucket.getEntryCount() );
            assertEquals( true, bucket.isCountTrusted() );
            assertEquals( 0, bucket.getAccurateEntryCount() );
            assertNull( bucket.get( generator.getAckId(1 ) ) );
            assertEquals( new StorePosition(bucket.getBucketId(), null), bucket.getPosition(generator.getAckId(1) ) );
        } finally {
            manager.close();
        }
        
        manager = createPersistenceManager(false);
        try {
            Collection<BucketPersistentMetaData> buckets = 
                    manager.getBucketMetadataForStore(STORE_NAME);
            
            assertEquals( 1, buckets.size() );
            
            Bucket bucket = null;
            try {
                bucket = BucketImpl.restoreBucket(
                        STORE_NAME, 
                        buckets.iterator().next(),
                        manager);
                
                assertNull( bucket.getFirstId() );
                assertNull( bucket.getLastId() );
                assertEquals( 0, bucket.getByteCount() );
                assertEquals( 0, bucket.getEntryCount() );
                assertEquals( false, bucket.isCountTrusted() );
                assertEquals( 0, bucket.getAccurateEntryCount() );
                assertEquals( true, bucket.isCountTrusted() );
                assertNull( bucket.get( generator.getAckId(1 ) ) );
                assertEquals( new StorePosition(bucket.getBucketId(), null), bucket.getPosition(generator.getAckId(1) ) );
            } finally {
                if( bucket != null ) bucket.close();
            }
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testReloadNonEmptyBucket() throws SeqStoreException, IOException {
        if( !useBDB ) return;
        
        StoredEntry<AckIdV3> entry, entry2;
        
        StorePersistenceManager manager = createPersistenceManager(true);
        try {
            Bucket bucket = createBucket(manager);
        
            entry = insertMessage( bucket, 5, 1 );
            assertStoredEntryEquals( entry, bucket.get( entry.getAckId() ) );
            manager.updateBucketMetadata(STORE_NAME, bucket.getMetadata() );
            
            // The manager will be closed without updating the metadata
            entry2 = insertMessage( bucket, 5, 1 );
        } finally {
            manager.close();
        }
        
        manager = createPersistenceManager(false);
        try {
            Collection<BucketPersistentMetaData> buckets = 
                    manager.getBucketMetadataForStore(STORE_NAME);
            
            assertEquals( 1, buckets.size() );
            
            Bucket bucket = null;
            try {
                bucket = BucketImpl.restoreBucket(
                        STORE_NAME, 
                        buckets.iterator().next(),
                        manager);
                
                assertStoredEntryEquals( entry, bucket.get( entry.getAckId() ) );
                assertStoredEntryEquals( entry2, bucket.get( entry2.getAckId() ) );
                
                // An entry that doesn't exist shouldn't get anything
                assertNull( bucket.get( generator.getAckId( 7 ) ) );
                
                assertEquals( entry.getAckId(), bucket.getFirstId() );
                assertEquals( entry2.getAckId(), bucket.getLastId() );
                // entry 2 wasn't included in the metadata
                assertEquals( entry.getPayloadSize() + AckIdV3.LENGTH, bucket.getByteCount() );
                assertEquals( 1, bucket.getEntryCount() );
                assertEquals( false, bucket.isCountTrusted() );
                // This should notice entry 2
                assertEquals( 2, bucket.getAccurateEntryCount() );
                assertEquals( true, bucket.isCountTrusted() );
            } finally {
                if( bucket != null ) bucket.close();
            }
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testGetPositionNonMatchingKeys() throws SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager(true);
        Bucket bucket = createBucket(manager);
        
        NavigableMap<AckIdV3, StoredEntry<AckIdV3>> entries = insertCommonTestEntries(bucket);
        
        // Test before all keys
        AckIdV3 beforeFirstId = generator.getAckId( entries.firstKey().getTime() - 1 );
        StorePosition expectedFirstPos = new StorePosition(bucket.getBucketId(), null);
        
        assertEquals( expectedFirstPos, bucket.getPosition( beforeFirstId ) );
        assertEquals( expectedFirstPos, bucket.getPosition( new AckIdV3( beforeFirstId, false ) ) );
        assertEquals( expectedFirstPos, bucket.getPosition( new AckIdV3( beforeFirstId, true ) ) );
        
        // Test after all keys
        AckIdV3 afterLastId = generator.getAckId( entries.lastKey().getTime() + 1 );
        StorePosition expectedLastPos = new StorePosition(bucket.getBucketId(), new AckIdV3( entries.lastKey(), entries.size() ) );
        
        assertEquals( expectedLastPos, bucket.getPosition( afterLastId ) );
        assertEquals( expectedLastPos, bucket.getPosition( new AckIdV3( afterLastId, false ) ) );
        assertEquals( expectedLastPos, bucket.getPosition( new AckIdV3( afterLastId, true ) ) );
        
        //  Test in the middle - start with id before the second id but after the first
        AckIdV3 middleId =
                generator.getAckId( entries.firstKey().getTime() + 1 );
        
        assertTrue( 
                "Error in test. First and second entries should be more than 1ms apart", 
                entries.headMap( middleId, true ).size() == 1 );
        
        int middlePos = 1;
        AckIdV3 middlePosId = entries.navigableKeySet().headSet(middleId, false).last();
        assertEquals( 
                new StorePosition( bucket.getBucketId(), new AckIdV3( middlePosId, middlePos ) ), 
                bucket.getPosition( middleId ) ) ;
        assertEquals( 
                new StorePosition( bucket.getBucketId(), new AckIdV3( middlePosId, middlePos ) ), 
                bucket.getPosition( new AckIdV3( middleId, false ) ) );
        assertEquals( 
                new StorePosition( bucket.getBucketId(), new AckIdV3( middlePosId, middlePos ) ), 
                bucket.getPosition( new AckIdV3( middleId, true ) ) );
    }
    
    /**
     * Tests getPosition with keys the have a bucket position but which don't actually exist in the bucket. These
     * shouldn't ever happen unless there is a bug but we should handle them anyway to be safe
     * 
     * @throws SeqStoreException
     * @throws IOException
     */
    @Test
    public void testGetPositionWithBadPositions() throws SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager(true);
        Bucket bucket = createBucket(manager);
        
        NavigableMap<AckIdV3, StoredEntry<AckIdV3>> entries = insertCommonTestEntries(bucket);
        
        // Test before all keys
        AckIdV3 beforeFirstId = new AckIdV3( generator.getAckId( entries.firstKey().getTime() - 1 ), 1 );
        StorePosition expectedFirstPos = new StorePosition(bucket.getBucketId(), null);
        
        assertEquals( expectedFirstPos, bucket.getPosition( beforeFirstId ) );
        assertEquals( expectedFirstPos, bucket.getPosition( new AckIdV3( beforeFirstId, false ) ) );
        assertEquals( expectedFirstPos, bucket.getPosition( new AckIdV3( beforeFirstId, true ) ) );
        
        // Test with a real key at the start - but which claims not to be the first entry
        assertEquals( expectedFirstPos, bucket.getPosition( new AckIdV3( entries.firstKey(), false, 2L ) ) );
        
        // Test after all keys
        AckIdV3 afterLastId = new AckIdV3( generator.getAckId( entries.lastKey().getTime() + 1 ), entries.size() + 1 );
        StorePosition expectedLastPos = new StorePosition(bucket.getBucketId(), new AckIdV3( entries.lastKey(), entries.size() ) );
        
        assertEquals( expectedLastPos, bucket.getPosition( afterLastId ) );
        assertEquals( expectedLastPos, bucket.getPosition( new AckIdV3( afterLastId, false ) ) );
        assertEquals( expectedLastPos, bucket.getPosition( new AckIdV3( afterLastId, true ) ) );
        
        //  Test in the middle - start with id before the second id but after the first
        AckIdV3 middleId =
                new AckIdV3( generator.getAckId( entries.firstKey().getTime() + 1 ), 2);
        
        assertTrue( 
                "Error in test. First and second entries should be more than 1ms apart", 
                entries.headMap( middleId, true ).size() == 1 );
        
        int middlePos = 1;
        AckIdV3 middlePosId = entries.navigableKeySet().headSet(middleId, false).last();
        assertEquals( 
                new StorePosition( bucket.getBucketId(), new AckIdV3( middlePosId, middlePos ) ), 
                bucket.getPosition( middleId ) ) ;
        assertEquals( 
                new StorePosition( bucket.getBucketId(), new AckIdV3( middlePosId, middlePos ) ), 
                bucket.getPosition( new AckIdV3( middleId, false ) ) );
        assertEquals( 
                new StorePosition( bucket.getBucketId(), new AckIdV3( middlePosId, middlePos ) ), 
                bucket.getPosition( new AckIdV3( middleId, true ) ) );
    }
    
    @Test
    public void testGetPositionExactKeys() throws SeqStoreException, IOException {
        @Cleanup
        StorePersistenceManager manager = createPersistenceManager(true);
        Bucket bucket = createBucket(manager);
        
        NavigableMap<AckIdV3, StoredEntry<AckIdV3>> entries = insertCommonTestEntries(bucket);
        
        // Test exact match
        long bucketOffset = 1;
        for( AckIdV3 ackId : entries.keySet() ) {
            StorePosition expectedPos =  new StorePosition(bucket.getBucketId(), new AckIdV3( ackId, bucketOffset ) );
            
            assertEquals( expectedPos, bucket.getPosition(ackId));
            assertEquals( expectedPos, bucket.getPosition( new AckIdV3( ackId, bucketOffset ) ) );
            bucketOffset++;
        }
        
        // Test not inclusive
        AckIdV3 previous = null;
        bucketOffset = 1;
        for( AckIdV3 ackId : entries.keySet() ) {
            StorePosition expectedPos = new StorePosition(bucket.getBucketId(), previous );
            
            assertEquals( expectedPos, bucket.getPosition( new AckIdV3( ackId, false ) ) );
            assertEquals( expectedPos, bucket.getPosition( new AckIdV3( ackId, false, bucketOffset ) ) );
            previous = new AckIdV3( ackId, bucketOffset );
            bucketOffset++;
        }
        
        // Test inclusive
        bucketOffset = 1;
        for( AckIdV3 ackId : entries.keySet() ) {
            StorePosition expectedPos =  new StorePosition(bucket.getBucketId(), new AckIdV3( ackId, bucketOffset ) );
            
            assertEquals( expectedPos, bucket.getPosition( new AckIdV3( ackId, true ) ) );
            assertEquals( expectedPos, bucket.getPosition( new AckIdV3( ackId, true, bucketOffset ) ) );
            bucketOffset++;
        }
    }
    
    private NavigableMap<AckIdV3, StoredEntry<AckIdV3>> insertCommonTestEntries(Bucket bucket)
        throws SeqStoreDatabaseException
    {
        NavigableMap< AckIdV3, StoredEntry<AckIdV3> > entries = new TreeMap< AckIdV3, StoredEntry<AckIdV3> >();
        
        insertMessage( entries, bucket, 6, 1);
        insertMessage( entries, bucket, 8, 1);
        insertMessage( entries, bucket, 4, 1);
        
        assertEquals( entries.firstKey(), bucket.getFirstId() );
        assertEquals( entries.lastKey(), bucket.getLastId() );
        return entries;
    }
}
