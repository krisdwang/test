package com.amazon.messaging.seqstore.v3.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.StoredCount;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.bdb.BDBPersistentManager;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.BucketStorageManager;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.LabelledParameterized;
import com.amazon.messaging.testing.LabelledParameterized.Parameters;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;
import com.amazon.messaging.utils.Scheduler;

@RunWith(LabelledParameterized.class)
public class BDBBucketManagerTest extends BucketManagerTestBase {
    private static final Logger log = Logger.getLogger(BDBBucketManagerTest.class);
    
    private static final StoreId storeId = new StoreIdImpl("test");
    
    @Parameters
    public static List<TestParameters> getParams() {
        return Arrays.asList( 
                new TestParameters("!UseLLMEnv", new Object[] { false } ),
                new TestParameters("UseLLMEnv", new Object[] { true } ) );
    }
    
    private BDBPersistentManager persistenceManager_;
    
    private BucketStorageManager bucketStorageManager_;

    private BucketManager bm_;
    
    public BDBBucketManagerTest(boolean createShared) {
        super( createShared );
    }

    @Override
    public BucketManager createBucketManager(Clock clock,
                                                    boolean clearExistingStore)
            throws IOException, SeqStoreException
    {
        createPersistenceManager(clearExistingStore);
        
        return createBucketManager();
    }
    
    @Override
    public StorePersistenceManager getPersistenceManager() {
        return persistenceManager_;
    }

    private void createPersistenceManager(boolean clearExistingStore)
        throws SeqStoreDatabaseException, IOException
    {
        assertEquals("Test attempted to have two active persistence managers at the same time", null, persistenceManager_);
        persistenceManager_ = new BDBPersistentManager(
                getPersistenceConfig(clearExistingStore), Scheduler.getGlobalInstance());
        
        bucketStorageManager_ = new BucketStorageManager(persistenceManager_, Scheduler.getGlobalInstance(), new NullMetricsFactory() );
    }

    private BucketManager createBucketManager() throws SeqStoreException {
        assertEquals("Test attempted to have two active bucket managers at the same time", null, bm_);
        
        bm_ = new BucketManager( storeId, STORE_CONFIG, bucketStorageManager_ );
        
        return bm_;
    }

    private SeqStoreImmutablePersistenceConfig getPersistenceConfig(boolean clearExistingStore) throws IOException {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        if (clearExistingStore) {
            con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        } else {
            con.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
        }
        con.setUseSeperateEnvironmentForDedicatedBuckets(useLLMEnv);
        return con.getImmutableConfig();
    }
    
    @After
    public void shutdown() throws Exception {
        if (bm_ != null) {
            log.warn("BucketManager was not cleanly shutdown.");
            try {
                closeBucketManager();
            } catch (SeqStoreException ex) {
                // Hide this as we don't want to hide previous failures that
                // may have been what caused the shutdown to fail.
                log.warn("Error when shutting down the bucket manager.", ex);
            }
        }
    }

    @Override
    protected void shutdownBucketManager(BucketManager bm) throws SeqStoreException {
        if ((bm_ == null) || (bm_ != bm)) {
            fail("Test attempted to shutdown inactive/non owned bucket manager.");
        }
        closeBucketManager();
    }

    private void closeBucketManager() throws SeqStoreException {
        assertTrue(bm_ != null);
        bm_.close();
        bucketStorageManager_.shutdown();
        persistenceManager_.close();
        bm_ = null;
        persistenceManager_ = null;
        bucketStorageManager_ = null;
    }

    @Test
    public void testRestoreBM() throws Exception {
        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries = new TreeMap<AckIdV3, StoredEntry<AckIdV3>>();
        int numMsg = random.nextInt(MAX_MSG);

        StoredCount initialCount;
        {
            BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

            enqueueMessages(bm, numMsg, entries);
            initialCount = bm.getCountAfter( null );
            assertEquals( numMsg, initialCount.getEntryCount() );
            closeBucketManager();
        }

        {
            BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), false);
            assertEquals( initialCount, bm.getCountAfter( null ) );
            
            int testTime = random.nextInt(MAX_DEQ_TIME);
            AckIdV3 testId = ackIdGen.getAckId(testTime);
            BucketJumper jumper = new BucketJumper(bm);
            
            AckIdV3 dest = jumper.advanceTo(testId);
            if (numMsg == 0) {
                assertNull(dest);
            } else {
                assertNotNull(dest);
                assertTrue(dest.compareTo(testId) <= 0);
            }
            // bm_.printout();
            StoredEntry<AckIdV3> next = jumper.next();
            while (next != null) {
                AckIdV3 beyondDest = next.getAckId();
                assertTrue(beyondDest.compareTo(testId) > 0);
                next = jumper.next();

            }

            closeBucketManager();
        }

        {
            BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), false);
            assertEquals( initialCount, bm.getCountAfter( null ) );
            
            int testTime = random.nextInt(MAX_DEQ_TIME);
            AckIdV3 testId = ackIdGen.getAckId(testTime);
            BucketJumper jumper = new BucketJumper(bm);
            AckIdV3 dest = jumper.advanceTo(testId);
            if (numMsg == 0) {
                assertTrue(dest == null);
            } else {
                assertTrue(dest.compareTo(testId) <= 0);
            }
            // bm_.printout();
            StoredEntry<AckIdV3> next = jumper.next();
            while (next != null) {
                AckIdV3 beyondDest = next.getAckId();
                assertTrue(beyondDest.compareTo(testId) > 0);
                next = jumper.next();
            }
            shutdownBucketManager(bm);
        }
    }
    
    @Test
    public void testEntryCountCorrectionAfterRestore() throws Exception {
        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries = new TreeMap<AckIdV3, StoredEntry<AckIdV3>>();
        int numMsg = (int) (5 * BucketManager.MIN_INACCURATE_COUNT);
        
        {
            BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

            enqueueMessages(bm, numMsg, entries);
            assertEquals( numMsg, bm.getCountAfter( null ).getEntryCount() );
            closeBucketManager();
        }
        
        {
            createPersistenceManager(false);
            
            // Mess up the metadata so it all got half the values it should
            for( BucketPersistentMetaData oldData : persistenceManager_.getBucketMetadataForStore(storeId) ) {
                BucketPersistentMetaData newData = 
                        new BucketPersistentMetaData(
                                oldData.getBucketSequenceId(), oldData.getBucketId(), oldData.getBucketStorageType(),
                                oldData.getEntryCount() / 2, oldData.getByteCount() / 2, false );
                persistenceManager_.updateBucketMetadata(storeId, newData );
            }
            
            BucketManager bm = createBucketManager();
            
            long count = bm.getCountAfter( null ).getEntryCount();
            assertTrue( count <= numMsg / 2 );
            assertTrue( count >= ( numMsg / 2 - bm.getNumBuckets() ) );
            
            // Enqueue some new messages
            enqueueMessages(bm, numMsg, entries);
            long newCount = bm.getCountAfter( null ).getEntryCount();
            assertEquals( count + numMsg, newCount );
            count = newCount;
            
            boolean recountHappened = false;
            BucketJumper jumper = new BucketJumper(bm);
            for( Map.Entry< AckIdV3, StoredEntry< AckIdV3 > > recordedEntry : entries.entrySet() ) {
                StoredEntry<AckIdV3> entry = jumper.next();
                assertNotNull( entry );
                assertEquals( recordedEntry.getKey(), entry.getAckId() );
                
                newCount = bm.getCountAfter( jumper.getPosition() ).getEntryCount();
                if( !recountHappened ) {
                    if( newCount > count ) {
                        assertEquals( BucketManager.MIN_INACCURATE_COUNT, count );
                        recountHappened = true;
                    } else {
                        assertTrue( ( newCount == count - 1 ) || ( newCount == count ) );
                        assertTrue( newCount >= BucketManager.MIN_INACCURATE_COUNT );
                    }
                }
                
                if( recountHappened ) {
                    assertEquals( entries.tailMap( entry.getAckId(), false ).size(), newCount );
                }
                count = newCount;
            }
            assertTrue( recountHappened );
            shutdownBucketManager(bm);
        }
    }

    @Test
    public void testDeleteOnRestore() throws Exception {
        NavigableMap< AckIdV3, StoredEntry< AckIdV3 > > entries = new TreeMap<AckIdV3, StoredEntry<AckIdV3>>();
        int numMsg = 2000;
        int numBuckets;
        
        {
            BucketManager bm = createBucketManager(new AlwaysIncreasingClock(), true);

            enqueueMessages(bm, numMsg, entries);
            assertEquals( numMsg, bm.getCountAfter( null ).getEntryCount() );
            numBuckets = bm.getNumBuckets();
            closeBucketManager();
        }
        
        {
            createPersistenceManager(false);
            
            // Mark the first bucket as deleted
            BucketPersistentMetaData oldData =
                    persistenceManager_.getBucketMetadataForStore(storeId).iterator().next();
            long firstBucketEntryCount = oldData.getEntryCount();
            BucketPersistentMetaData newData = 
                    new BucketPersistentMetaData(
                            oldData.getBucketSequenceId(), oldData.getBucketId(), oldData.getBucketStorageType(),
                            oldData.getEntryCount(), oldData.getByteCount(), true );
            persistenceManager_.updateBucketMetadata(storeId, newData );
            
            BucketManager bm = createBucketManager();
            assertEquals( numBuckets - 1, bm.getNumBuckets() );
            bucketStorageManager_.waitForDeletesToFinish(1, TimeUnit.SECONDS);
            assertEquals( numBuckets - 1, bucketStorageManager_.getNumBuckets() );
            
            int pos = 0;
            BucketJumper jumper = new BucketJumper(bm);
            for( Map.Entry< AckIdV3, StoredEntry< AckIdV3 > > recordedEntry : entries.entrySet() ) {
                if( pos < firstBucketEntryCount ) continue;
                
                StoredEntry<AckIdV3> entry = jumper.next();
                assertNotNull( entry );
                assertEquals( recordedEntry.getKey(), entry.getAckId() );
            }
            
            shutdownBucketManager(bm);
        }
    }
}
