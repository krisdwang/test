package com.amazon.messaging.seqstore.v3.bdb;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.measure.unit.NonSI;
import javax.measure.unit.Unit;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreAlreadyCreatedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDeleteInProgressException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData.BucketState;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.testing.DeterministicScheduledExecutorService;
import com.amazon.messaging.testing.LabelledParameterized;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.testing.LabelledParameterized.Parameters;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;
import com.amazon.messaging.util.BDBBucketTestParameters;
import com.amazon.messaging.utils.Scheduler;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;

import org.mockito.Mockito;

import static org.mockito.Mockito.*;

@RunWith(LabelledParameterized.class)
public class BDBPersistenceManagerTest extends TestCase {

    private final int dirtyMetadataFlushPeriodMS = 20000;

    private Scheduler scheduler;

    private DeterministicScheduledExecutorService executorService = new DeterministicScheduledExecutorService();

    private BucketStorageType bucketStorageType;

    private boolean useLLM;

    @Parameters
    public static List<TestParameters> getTestParameters() {
        return BDBBucketTestParameters.getAllOptionsTestParameters();
    }

    public BDBPersistenceManagerTest(BucketStorageType bucketStorageType, boolean useLLM) {
        this.bucketStorageType = bucketStorageType;
        this.useLLM = useLLM;
    }

    @Before
    public void beforeTest() {
        scheduler = new Scheduler("BDBPersistenceManagerTest", executorService);
    }

    @After
    public void afterTest() {
        assertEquals(0, scheduler.numRemainingTasks());
        scheduler.shutdown();
    }

    public BDBPersistentManager getManager(boolean truncate) throws IOException, SeqStoreDatabaseException {
        return getManager(useLLM, truncate);
    }

    public BDBPersistentManager getManager(boolean useLLM, boolean truncate)
        throws IOException, SeqStoreDatabaseException
    {
        return getManager(useLLM, truncate, false);
    }

    public BDBPersistentManager getManager(boolean useLLM, boolean truncate, boolean spy)
        throws IOException, SeqStoreDatabaseException
    {
        SeqStorePersistenceConfig con = getManagerConfig(useLLM, truncate);
        
        SeqStoreImmutablePersistenceConfig immutableConfig = con.getImmutableConfig();
        if (spy) {
            return new BDBPersistentManager(immutableConfig, scheduler) {
                @Override
                Environment createBDBEnvironment(File storeDirectory, EnvironmentConfig envConfig) {
                    return Mockito.spy(new Environment(storeDirectory, envConfig));
                }
            };
        } else {
            return new BDBPersistentManager(immutableConfig, scheduler);
        }
    }

    private SeqStorePersistenceConfig getManagerConfig(boolean useLLM, boolean truncate) throws IOException {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        if (truncate) {
            con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        } else {
            con.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
        }
        con.setUseSeperateEnvironmentForDedicatedBuckets(useLLM);
        con.setDirtyMetadataFlushPeriod(dirtyMetadataFlushPeriodMS);
        return con;
    }

    @Test
    public void testIsHealthy() throws IOException, SeqStoreException {
        BDBPersistentManager manager = getManager(true);
        try {
            assertTrue(manager.isHealthy());
        } finally {
            manager.close();
        }

        assertFalse(manager.isHealthy());
    }
    
    @Test
    public void testGetSupportedBucketTypes() throws SeqStoreDatabaseException, IOException, SeqStoreInternalException {
        BDBPersistentManager manager = getManager(true);
        try {
            Set<BucketStorageType> supportedTypes = manager.getSupportedNewBucketTypes();
            assertTrue( supportedTypes.contains( BucketStorageType.SharedDatabase ) );
            assertTrue( supportedTypes.contains( BucketStorageType.DedicatedDatabase ) );
            
            if( useLLM ) {
                assertTrue( supportedTypes.contains( BucketStorageType.DedicatedDatabaseInLLMEnv ) );    
            } else {
                assertFalse( supportedTypes.contains( BucketStorageType.DedicatedDatabaseInLLMEnv ) );
            }
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testRunBDBCleanup() throws SeqStoreDatabaseException, IOException, SeqStoreInternalException {
        BDBPersistentManager manager = getManager(true, true, true);
        try {
            manager.runBDBCleanup();
            verify( manager.getMainEnvironment() ).cleanLog();
            if( useLLM ) {
                verify( manager.getLongLivedMessagesEnvironment() ).cleanLog();
            }
        } finally {
            manager.close();
        }
    }
    
    /**
     * Do some basic validation of reportPerformanceMetrics
     * @throws SeqStoreDatabaseException
     * @throws IOException
     * @throws SeqStoreInternalException
     * @throws SeqStoreDeleteInProgressException 
     * @throws SeqStoreAlreadyCreatedException 
     */
    @Test
    public void testReportPerformanceMetrics() 
            throws SeqStoreDatabaseException, IOException, SeqStoreInternalException, SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException 
    {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        BDBPersistentManager manager = getManager(true);
        try {
            Metrics metrics = mock(Metrics.class);
            manager.reportPerformanceMetrics(metrics);
            verify(metrics).addCount(eq("bdbSize"), doubleThat(Matchers.greaterThan(0d)), eq(NonSI.BYTE) );
            verify(metrics).addCount("OpenBuckets", 0, Unit.ONE);
            verify(metrics).addCount("OpenDedicatedBuckets", 0, Unit.ONE);
            verify(metrics).addCount("SeqStores", 0, Unit.ONE);
            verify(metrics, never()).close();
            
            // Create a store
            manager.createStore(storeId, storeAckLevels);

            // Create a bucket
            BucketStore bucketStore = manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
            assertNotNull(bucketStore);

            metrics = mock(Metrics.class);
            manager.reportPerformanceMetrics(metrics);
            verify(metrics).addCount(eq("bdbSize"), doubleThat(Matchers.greaterThan(0d)), eq(NonSI.BYTE) );
            verify(metrics).addCount("OpenBuckets", 1, Unit.ONE);
            verify(metrics).addCount("OpenDedicatedBuckets", getExpectedStorageType().isDedicated() ? 1 : 0, Unit.ONE);
            verify(metrics).addCount("SeqStores", 1, Unit.ONE);
            verify(metrics, never()).close();
        } finally {
            manager.close();
        }
    }

    @Test
    public void testStoreAndRetrieveAckLevels() throws IOException, SeqStoreException {
        StoreId storeId1 = new StoreIdImpl("storeGroup1", null);
        StoreId storeId2 = new StoreIdImpl("storeGroup11", null);

        Map<String, AckIdV3> store1AckLevels = new HashMap<String, AckIdV3>();
        store1AckLevels.put("reader", new AckIdV3(0, false));

        Map<String, AckIdV3> store2AckLevels = new HashMap<String, AckIdV3>();
        store1AckLevels.put("reader", new AckIdV3(5, false));

        BDBPersistentManager manager = getManager(true);
        try {
            assertFalse(manager.containsStore(storeId1));
            assertFalse(manager.containsStore(storeId2));

            assertNull(manager.getReaderLevels(storeId1));
            assertNull(manager.getReaderLevels(storeId2));

            manager.createStore(storeId1, store1AckLevels);

            assertTrue(manager.containsStore(storeId1));
            assertFalse(manager.containsStore(storeId2));

            Map<String, AckIdV3> persistedStore1AckLevels = manager.getReaderLevels(storeId1);
            assertNotSame(store1AckLevels, persistedStore1AckLevels);
            assertEquals(store1AckLevels, persistedStore1AckLevels);
            assertNull(manager.getReaderLevels(storeId2));

            store1AckLevels.put("reader", new AckIdV3(8, false));
            store1AckLevels.put("reader2", new AckIdV3(5, false));
            // The update to the map shouldn't affect the persisted levels
            assertFalse(store1AckLevels.equals(persistedStore1AckLevels));
            // even if they are fetched again
            persistedStore1AckLevels = manager.getReaderLevels(storeId1);
            assertFalse(store1AckLevels.equals(persistedStore1AckLevels));

            manager.persistReaderLevels(storeId1, store1AckLevels);
            // persistedStore1AckLevels should be a snapshot and not be updated
            // when the persisted levels are updated
            assertFalse(store1AckLevels.equals(persistedStore1AckLevels));

            // Only when fetched again should the update be seen
            persistedStore1AckLevels = manager.getReaderLevels(storeId1);
            assertEquals(store1AckLevels, persistedStore1AckLevels);

            // store2 hasn't been persisted so it shouldn't exist yet
            assertFalse(manager.containsStore(storeId2));
            assertNull(manager.getReaderLevels(storeId2));

            manager.createStore(storeId2, store2AckLevels);

            Map<String, AckIdV3> persistedStore2AckLevels = manager.getReaderLevels(storeId2);
            assertEquals(store2AckLevels, persistedStore2AckLevels);

            // The store1 levels shouldn't have changed
            persistedStore1AckLevels = manager.getReaderLevels(storeId1);
            assertEquals(store1AckLevels, persistedStore1AckLevels);
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            // Make sure the levels are remembered after a restart
            Map<String, AckIdV3> persistedStore1AckLevels = manager.getReaderLevels(storeId1);
            assertEquals(store1AckLevels, persistedStore1AckLevels);

            Map<String, AckIdV3> persistedStore2AckLevels = manager.getReaderLevels(storeId2);
            assertEquals(store2AckLevels, persistedStore2AckLevels);
        } finally {
            manager.close();
        }
    }

    @Test
    public void testStoreGroupManagement() throws IOException, SeqStoreException {
        String group1Name = "storeGroup1";
        String group2Name = "storeGroup11";

        StoreId storeId1 = new StoreIdImpl(group1Name, null);
        StoreId storeId2 = new StoreIdImpl(group2Name, null);
        StoreId storeId3 = new StoreIdImpl(group1Name, "p1:p2");

        Map<String, AckIdV3> emptyMap = Collections.emptyMap();

        Set<String> expectedGroups = new HashSet<String>();
        Set<StoreId> expectedStores = new HashSet<StoreId>();
        Set<StoreId> expectedGroup1Stores = new HashSet<StoreId>();
        Set<StoreId> expectedGroup2Stores = new HashSet<StoreId>();

        BDBPersistentManager manager = getManager(true);
        try {
            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));

            manager.createStore(storeId1, emptyMap);
            expectedGroups.add(group1Name);
            expectedStores.add(storeId1);
            expectedGroup1Stores.add(storeId1);

            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));

            manager.createStore(storeId2, emptyMap);
            expectedGroups.add(group2Name);
            expectedStores.add(storeId2);
            expectedGroup2Stores.add(storeId2);

            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));

            manager.createStore(storeId3, emptyMap);
            expectedStores.add(storeId3);
            expectedGroup1Stores.add(storeId3);

            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));

            manager.deleteStoreWithNoBuckets(storeId1);
            expectedStores.remove(storeId1);
            expectedGroup1Stores.remove(storeId1);

            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));

            manager.storeDeletionStarted(storeId2);
            expectedStores.remove(storeId2);
            expectedGroup2Stores.remove(storeId2);
            expectedGroups.remove(group2Name);

            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));

            manager.storeDeletionCompleted(storeId2);

            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));

            manager.deleteStoreWithNoBuckets(storeId3);
            expectedStores.remove(storeId3);
            expectedGroup1Stores.remove(storeId3);
            expectedGroups.remove(group1Name);

            assertEquals(expectedStores, manager.getStoreIds());
            assertEquals(expectedGroups, manager.getGroups());
            assertEquals(expectedGroup1Stores, manager.getStoreIdsForGroup(group1Name));
            assertEquals(expectedGroup2Stores, manager.getStoreIdsForGroup(group2Name));
        } finally {
            manager.close();
        }
    }

    private static void assertMatchingMetadataCollection(
        Collection<BucketPersistentMetaData> expected, Collection<BucketPersistentMetaData> actual)
    {
        assertEquals(new HashSet<BucketPersistentMetaData>(expected), new HashSet<BucketPersistentMetaData>(
                actual));
    }

    @Test
    public void testBucketCreateAndDelete() throws IOException, SeqStoreException {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        AckIdGenerator ackIdGen = new AckIdGenerator(5);

        AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
        StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));

        long bucketSequenceId;

        BDBPersistentManager manager = getManager(true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());
            assertEquals(0, manager.getNumBuckets());
            assertEquals(0, manager.getNumDedicatedBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            BucketStore bucketStore = manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
            assertNotNull(bucketStore);

            assertEquals(0, bucketStore.getCount());
            assertNull(bucketStore.getFirstId());
            assertNull(bucketStore.getLastId());

            assertEquals(1, manager.getNumOpenBuckets());
            assertEquals(1, manager.getNumBuckets());
            assertEquals( getExpectedStorageType().isDedicated() ? 1 : 0, manager.getNumDedicatedBuckets());

            bucketSequenceId = bucketStore.getSequenceId();

            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertEquals(expectedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            bucketStore.put(storedId1, storedEntry1, true);

            assertEquals(1, bucketStore.getCount());

            manager.closeBucketStore(storeId, bucketStore);
            assertEquals(0, manager.getNumOpenBuckets());

            bucketStore = manager.getBucketStore(storeId, bucketId);
            assertEquals(1, bucketStore.getCount());
            assertEquals(1, manager.getNumOpenBuckets());
            assertEquals(1, manager.getNumBuckets());
            assertEquals( getExpectedStorageType().isDedicated() ? 1 : 0, manager.getNumDedicatedBuckets());

            BucketStore bucketStore2 = manager.getBucketStore(storeId, bucketId);
            assertEquals(1, bucketStore2.getCount());
            assertEquals(1, manager.getNumOpenBuckets());
            assertEquals(1, manager.getNumBuckets());
            assertEquals( getExpectedStorageType().isDedicated() ? 1 : 0, manager.getNumDedicatedBuckets());
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            assertEquals(0, manager.getNumOpenBuckets());
            assertEquals(1, manager.getNumBuckets());
            assertEquals( getExpectedStorageType().isDedicated() ? 1 : 0, manager.getNumDedicatedBuckets());
            
            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertMatchingMetadataCollection(
                    Collections.singleton(expectedMetadata), manager.getBucketMetadataForStore(storeId));

            BucketStore bucketStore = manager.getBucketStore(storeId, bucketId);
            assertEquals(bucketId, bucketStore.getBucketId());

            assertEquals(1, bucketStore.getCount());
            manager.closeBucketStore(storeId, bucketStore);

            manager.deleteBucketStore(storeId, bucketId);

            assertEquals(0, manager.getNumOpenBuckets());
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
            assertEquals(0, manager.getSharedDatabaseSize());
            assertEquals(0, manager.getNumBuckets());
            assertEquals(0, manager.getNumDedicatedBuckets());
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            assertEquals(0, manager.getSharedDatabaseSize());
            assertEquals(0, manager.getNumBuckets());
            assertEquals(0, manager.getNumDedicatedBuckets());
            // The bucket shouldn't show up after restart if it's been deleted
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }
    }

    @Test
    public void testBucketMetadata()
        throws SeqStoreDatabaseException, IOException, SeqStoreInternalException,
        SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException
    {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        BucketPersistentMetaData updatedMetadata;
        BDBPersistentManager manager = getManager(true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            BucketStore bucketStore = manager.createBucketStore(storeId, bucketId, bucketStorageType);
            assertNotNull(bucketStore);

            long bucketSequenceId = bucketStore.getSequenceId();

            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertEquals(expectedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            updatedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 1, 5, false);
            boolean result = manager.updateBucketMetadata(storeId, updatedMetadata);
            assertTrue(result);

            assertEquals(updatedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            assertMatchingMetadataCollection(
                    Collections.singleton(updatedMetadata), manager.getBucketMetadataForStore(storeId));
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            assertMatchingMetadataCollection(
                    Collections.singleton(updatedMetadata), manager.getBucketMetadataForStore(storeId));

            assertEquals(updatedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            BucketPersistentMetaData markedDeletedMetadata = new BucketPersistentMetaData(
                    updatedMetadata.getBucketSequenceId(), updatedMetadata.getBucketId(),
                    updatedMetadata.getBucketStorageType(), updatedMetadata.getEntryCount(),
                    updatedMetadata.getByteCount(), true);
            boolean result = manager.updateBucketMetadata(storeId, markedDeletedMetadata);
            assertTrue(result);

            assertEquals(markedDeletedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            // Updating a marked deleted bucket's metadata shouldn't do anything
            result = manager.updateBucketMetadata(storeId, updatedMetadata);
            assertFalse(result);

            assertEquals(markedDeletedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            // Actually delete the bucket
            manager.deleteBucketStore(storeId, bucketId);

            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
            assertEquals(null, manager.getPersistedBucketData(storeId, bucketId));

            // Updating a deleted bucket's metadata shouldn't do anything
            result = manager.updateBucketMetadata(storeId, updatedMetadata);
            assertFalse(result);

            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
            assertEquals(null, manager.getPersistedBucketData(storeId, bucketId));

            // Neither should updating a bucket that never existed
            AckIdV3 newBucketId = new AckIdV3(10, true);
            BucketPersistentMetaData neverExistedBucketMetadata = new BucketPersistentMetaData(
                    updatedMetadata.getBucketSequenceId() + 1, newBucketId, getExpectedStorageType(), 1, 5,
                    false);
            result = manager.updateBucketMetadata(storeId, neverExistedBucketMetadata);
            assertFalse(result);

            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
            assertEquals(null, manager.getPersistedBucketData(storeId, newBucketId));
        } finally {
            manager.close();
            manager = null;
        }
    }

    @Test
    public void testBDBExists() throws SeqStoreDatabaseException, IOException, SeqStoreInternalException {
        SeqStoreImmutablePersistenceConfig config;

        BDBPersistentManager manager = getManager(true);
        try {
            config = manager.getConfig();
            assertTrue(BDBPersistentManager.bdbExists(config));
        } finally {
            manager.close();
            manager = null;
        }

        assertTrue(BDBPersistentManager.bdbExists(config));
        SeqStoreTestUtils.clearDirectory(config.getStoreDirectory());
        assertFalse(BDBPersistentManager.bdbExists(config));
    }

    private BucketStorageType getExpectedStorageType() {
        if (!useLLM && bucketStorageType == BucketStorageType.DedicatedDatabaseInLLMEnv) {
            return BucketStorageType.DedicatedDatabase;
        } else {
            return bucketStorageType;
        }
    }

    @Test
    public void testStoreCreationAndDeletionWithNoBuckets()
        throws SeqStoreDatabaseException, IOException, SeqStoreInternalException,
        SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException
    {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        BDBPersistentManager manager = getManager(true);
        try {
            manager.createStore(storeId, storeAckLevels);

            try {
                manager.createStore(storeId, storeAckLevels);
                fail("Duplicate create store called did not fail");
            } catch (SeqStoreAlreadyCreatedException e) {
                // Success
            }

            assertEquals(storeAckLevels, manager.getReaderLevels(storeId));
            assertTrue(manager.containsStore(storeId));
            assertEquals(Collections.singleton(storeId), manager.getStoreIds());
            assertEquals(Collections.emptySet(), manager.getStoreIdsBeingDeleted());

            Map<String, AckIdV3> newAckLevels = new HashMap<String, AckIdV3>();
            newAckLevels.put("reader", new AckIdV3(1, false));

            manager.persistReaderLevels(storeId, newAckLevels);
            assertEquals(newAckLevels, manager.getReaderLevels(storeId));

            // Delete the store
            manager.deleteStoreWithNoBuckets(storeId);

            assertNull(manager.getReaderLevels(storeId));
            assertFalse(manager.containsStore(storeId));
            assertEquals(Collections.emptySet(), manager.getStoreIds());
            assertEquals(Collections.emptySet(), manager.getStoreIdsBeingDeleted());

            // A second delete should do nothing
            manager.deleteStoreWithNoBuckets(storeId);

            try {
                manager.persistReaderLevels(storeId, newAckLevels);
                fail("Allowed to persist reader levels after store has been deleted");
            } catch (IllegalArgumentException e) {
                // Success
            }

            assertNull(manager.getReaderLevels(storeId));

            // Recreate should be allowed
            manager.createStore(storeId, newAckLevels);

            assertEquals(newAckLevels, manager.getReaderLevels(storeId));
        } finally {
            manager.close();
            manager = null;
        }
    }

    @Test
    public void testStoreCreationAndDeletionWithBuckets()
        throws SeqStoreDatabaseException, IOException, SeqStoreInternalException,
        SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException
    {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        BDBPersistentManager manager = getManager(true);
        try {
            manager.createStore(storeId, storeAckLevels);

            assertEquals(storeAckLevels, manager.getReaderLevels(storeId));
            assertTrue(manager.containsStore(storeId));
            assertEquals(Collections.singleton(storeId), manager.getStoreIds());
            assertEquals(Collections.emptySet(), manager.getStoreIdsBeingDeleted());

            BucketStore bucketStore1 = manager.createBucketStore(storeId, bucketId, bucketStorageType);
            assertNotNull(bucketStore1);

            manager.closeBucketStore(storeId, bucketStore1);

            try {
                manager.deleteStoreWithNoBuckets(storeId);
                fail("Deletion of store with buckets was not blocked");
            } catch (IllegalStateException e) {
                // Success
            }

            // The failed delete shouldn't have changed anything
            assertEquals(storeAckLevels, manager.getReaderLevels(storeId));
            assertTrue(manager.containsStore(storeId));
            assertEquals(Collections.singleton(storeId), manager.getStoreIds());
            assertEquals(Collections.emptySet(), manager.getStoreIdsBeingDeleted());

            boolean firstDelete = manager.storeDeletionStarted(storeId);
            assertTrue(firstDelete);

            // The store should not longer show up in the store set
            assertNull(manager.getReaderLevels(storeId));
            assertFalse(manager.containsStore(storeId));
            assertEquals(Collections.emptySet(), manager.getStoreIds());
            assertEquals(Collections.singleton(storeId), manager.getStoreIdsBeingDeleted());

            assertFalse(manager.storeDeletionStarted(storeId));

            assertNull(manager.getReaderLevels(storeId));
            assertFalse(manager.containsStore(storeId));
            assertEquals(Collections.emptySet(), manager.getStoreIds());
            assertEquals(Collections.singleton(storeId), manager.getStoreIdsBeingDeleted());

            try {
                manager.storeDeletionCompleted(storeId);
                fail("storeDeletionCompleted was allowed with buckets still existing");
            } catch (IllegalStateException e) {
                // Success
            }

            try {
                manager.createStore(storeId, storeAckLevels);
                fail("createStore was allowed before deletion was complete");
            } catch (SeqStoreDeleteInProgressException e) {
                // Success
            }

            Collection<BucketPersistentMetaData> markedBuckets = manager.markAllBucketsForStoreAsPendingDeletion(storeId);
            assertMatchingMetadataCollection(Collections.singleton(new BucketPersistentMetaData(
                    bucketStore1.getSequenceId(), bucketId, getExpectedStorageType(),
                    BucketPersistentMetaData.BucketState.DELETED)), markedBuckets);

            try {
                manager.storeDeletionCompleted(storeId);
                fail("storeDeletionCompleted was allowed with buckets still existing");
            } catch (IllegalStateException e) {
                // Success
            }

            try {
                manager.createStore(storeId, storeAckLevels);
                fail("createStore was allowed before deletion was complete");
            } catch (SeqStoreDeleteInProgressException e) {
                // Success
            }

            assertNull(manager.getReaderLevels(storeId));
            assertFalse(manager.containsStore(storeId));
            assertEquals(Collections.emptySet(), manager.getStoreIds());
            assertEquals(Collections.singleton(storeId), manager.getStoreIdsBeingDeleted());

            manager.deleteBucketStore(storeId, bucketId);

            // Really do the delete
            manager.storeDeletionCompleted(storeId);

            assertNull(manager.getReaderLevels(storeId));
            assertFalse(manager.containsStore(storeId));
            assertEquals(Collections.emptySet(), manager.getStoreIds());
            assertEquals(Collections.emptySet(), manager.getStoreIdsBeingDeleted());

            manager.createStore(storeId, storeAckLevels);

            assertEquals(storeAckLevels, manager.getReaderLevels(storeId));
            assertTrue(manager.containsStore(storeId));
            assertEquals(Collections.singleton(storeId), manager.getStoreIds());
            assertEquals(Collections.emptySet(), manager.getStoreIdsBeingDeleted());
        } finally {
            manager.close();
            manager = null;
        }
    }

    @Test
    public void testBulkMarkPendingDeleted()
        throws SeqStoreDatabaseException, IOException, SeqStoreInternalException,
        SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException
    {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);
        AckIdGenerator ackIdGen = new AckIdGenerator(15);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId1 = new AckIdV3(5, true);
        AckIdV3 bucketId2 = new AckIdV3(15, true);

        BDBPersistentManager manager = getManager(true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            manager.createStore(storeId, storeAckLevels);

            BucketStore bucketStore1 = manager.createBucketStore(storeId, bucketId1, bucketStorageType);
            assertNotNull(bucketStore1);
            
            BucketStore bucketStore2 = manager.createBucketStore(storeId, bucketId2, bucketStorageType);
            assertNotNull(bucketStore2);

            AckIdV3 storedId1 = ackIdGen.getAckId(17, 10);
            StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));
            bucketStore2.put(storedId1, storedEntry1, false);

            assertEquals(1, bucketStore2.getCount());

            manager.closeBucketStore(storeId, bucketStore1);
            manager.closeBucketStore(storeId, bucketStore2);

            long bucketSequenceId1 = bucketStore1.getSequenceId();
            long bucketSequenceId2 = bucketStore2.getSequenceId();

            assertTrue(bucketSequenceId1 != bucketSequenceId2);

            assertEquals(getExpectedStorageType(), bucketStore1.getBucketStorageType());
            assertEquals(getExpectedStorageType(), bucketStore2.getBucketStorageType());

            assertEquals(Collections.singleton(storeId), manager.getStoreIds());

            List<BucketPersistentMetaData> expectedMetadata = new ArrayList<BucketPersistentMetaData>();
            expectedMetadata.add(new BucketPersistentMetaData(
                    bucketSequenceId1, bucketId1, getExpectedStorageType(),
                    BucketPersistentMetaData.BucketState.ACTIVE));
            expectedMetadata.add(new BucketPersistentMetaData(
                    bucketSequenceId2, bucketId2, getExpectedStorageType(),
                    BucketPersistentMetaData.BucketState.ACTIVE));
            assertMatchingMetadataCollection(expectedMetadata, manager.getBucketMetadataForStore(storeId));

            // Start the store deletion
            manager.storeDeletionStarted(storeId);
            assertEquals(Collections.singleton(storeId), manager.getStoreIdsBeingDeleted());

            // Mark all the buckets
            Collection<BucketPersistentMetaData> markedBuckets = manager.markAllBucketsForStoreAsPendingDeletion(storeId);

            for (int i = 0; i < expectedMetadata.size(); ++i) {
                BucketPersistentMetaData metadata = expectedMetadata.get(i);
                expectedMetadata.set(i, metadata.getDeletedMetadata());
            }

            assertMatchingMetadataCollection(expectedMetadata, markedBuckets);
            assertMatchingMetadataCollection(expectedMetadata, manager.getBucketMetadataForStore(storeId));

            manager.deleteBucketStore(storeId, bucketId1);
            manager.deleteBucketStore(storeId, bucketId2);

            assertEquals(Collections.emptySet(), manager.getStoreIds());
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
            assertNull(manager.getPersistedBucketData(storeId, bucketId1));
            assertNull(manager.getPersistedBucketData(storeId, bucketId2));

            manager.storeDeletionCompleted(storeId);

            // Recreate the store and verify nothing carried over
            manager.createStore(storeId, storeAckLevels);

            assertTrue(manager.getStoreIdsBeingDeleted().isEmpty());

            bucketStore1 = manager.createBucketStore(storeId, bucketId1, bucketStorageType);
            assertNotNull(bucketStore1);

            bucketStore2 = manager.createBucketStore(storeId, bucketId2, bucketStorageType);
            assertNotNull(bucketStore2);

            assertEquals(0, bucketStore1.getCount());
            assertEquals(0, bucketStore2.getCount());
        } finally {
            manager.close();
            manager = null;
        }
    }
    
    /**
     * Test that buckets created with useLLM can still be loaded
     * when useLLM is false, and the other way round
     * 
     * @throws IOException
     * @throws SeqStoreException
     */
    @Test
    public void testChangingUseLLM() throws IOException, SeqStoreException {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        AckIdGenerator ackIdGen = new AckIdGenerator(5);

        AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
        StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));

        long bucketSequenceId;

        BDBPersistentManager manager = getManager(useLLM, true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            BucketStore bucketStore = manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
            assertNotNull(bucketStore);

            assertEquals(0, bucketStore.getCount());
            assertNull(bucketStore.getFirstId());
            assertNull(bucketStore.getLastId());

            assertEquals(1, manager.getNumOpenBuckets());

            bucketSequenceId = bucketStore.getSequenceId();

            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertEquals(expectedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            bucketStore.put(storedId1, storedEntry1, true);

            assertEquals(1, bucketStore.getCount());

            manager.closeBucketStore(storeId, bucketStore);
            assertEquals(0, manager.getNumOpenBuckets());

            bucketStore = manager.getBucketStore(storeId, bucketId);
            assertEquals(1, bucketStore.getCount());
            assertEquals(1, manager.getNumOpenBuckets());

            BucketStore bucketStore2 = manager.getBucketStore(storeId, bucketId);
            assertEquals(1, bucketStore2.getCount());
            assertEquals(1, manager.getNumOpenBuckets());
        } finally {
            manager.close();
            manager = null;
        }

        // Swapping if llm buckets are used or not should not break anything for
        // existing buckets
        manager = getManager(!useLLM, false);
        try {
            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertMatchingMetadataCollection(
                    Collections.singleton(expectedMetadata), manager.getBucketMetadataForStore(storeId));
            assertEquals(0, manager.getNumOpenBuckets());

            BucketStore bucketStore = manager.getBucketStore(storeId, bucketId);
            assertEquals(bucketId, bucketStore.getBucketId());

            assertEquals(1, bucketStore.getCount());
            manager.closeBucketStore(storeId, bucketStore);

            manager.deleteBucketStore(storeId, bucketId);

            assertEquals(0, manager.getNumOpenBuckets());
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            // The bucket shouldn't show up after restart if it's been deleted
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }
    }
    
    /**
     * Test that if useLLM is false no buckets are created that will break after a rollback
     * 
     * @throws IOException
     * @throws SeqStoreException
     */
    @Test
    public void testBackwardsCompatibility() throws IOException, SeqStoreException {
        Assume.assumeTrue(!useLLM);
        
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        AckIdGenerator ackIdGen = new AckIdGenerator(5);

        AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
        StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));

        long bucketSequenceId;

        BDBPersistentManager manager = getManager(false, true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            BucketStore bucketStore = manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
            assertNotNull(bucketStore);

            assertEquals(0, bucketStore.getCount());
            assertNull(bucketStore.getFirstId());
            assertNull(bucketStore.getLastId());

            assertEquals(1, manager.getNumOpenBuckets());

            bucketSequenceId = bucketStore.getSequenceId();

            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertEquals(expectedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            bucketStore.put(storedId1, storedEntry1, true);

            assertEquals(1, bucketStore.getCount());

            manager.closeBucketStore(storeId, bucketStore);
            assertEquals(0, manager.getNumOpenBuckets());

            bucketStore = manager.getBucketStore(storeId, bucketId);
            assertEquals(1, bucketStore.getCount());
            assertEquals(1, manager.getNumOpenBuckets());

            BucketStore bucketStore2 = manager.getBucketStore(storeId, bucketId);
            assertEquals(1, bucketStore2.getCount());
            assertEquals(1, manager.getNumOpenBuckets());
        } finally {
            manager.close();
            manager = null;
        }

        
        System.setProperty(BDBPersistentManager.FAIL_IF_LLM_FEATURES_USED_PROPERTY, "true");
        try {
            manager = getManager(false, false);
            try {
                BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                        bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
                assertMatchingMetadataCollection(
                        Collections.singleton(expectedMetadata), manager.getBucketMetadataForStore(storeId));
                assertEquals(0, manager.getNumOpenBuckets());

                BucketStore bucketStore = manager.getBucketStore(storeId, bucketId);
                assertEquals(bucketId, bucketStore.getBucketId());

                assertEquals(1, bucketStore.getCount());
                manager.closeBucketStore(storeId, bucketStore);

                manager.deleteBucketStore(storeId, bucketId);

                assertEquals(0, manager.getNumOpenBuckets());
                assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
            } finally {
                manager.close();
                manager = null;
            }
        } finally {
            System.setProperty(BDBPersistentManager.FAIL_IF_LLM_FEATURES_USED_PROPERTY, "");
        }
    }
    
    @Test
    public void testDuplicateCreate() 
            throws SeqStoreDatabaseException, IOException, SeqStoreAlreadyCreatedException,
            SeqStoreDeleteInProgressException, SeqStoreInternalException
    {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        BDBPersistentManager manager = getManager(true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            BucketStore bucketStore = manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
            assertNotNull(bucketStore);

            assertEquals(0, bucketStore.getCount());
            assertNull(bucketStore.getFirstId());
            assertNull(bucketStore.getLastId());

            assertEquals(1, manager.getNumOpenBuckets());
            
            try {
                manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                fail( "Duplicate create succeeeded");
            } catch( IllegalStateException e ) {
            }
        } finally {
            manager.close();
            manager = null;
        }

        // Check that it still isn't possible after a restart
        manager = getManager(false);
        try {
            try {
                manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                fail( "Duplicate create succeeeded");
            } catch( IllegalStateException e ) {
            }
        } finally {
            manager.close();
            manager = null;
        }
    }

    @Test
    public void testLLMDedicatedBucketCreationFailure()
        throws SeqStoreDatabaseException, IOException, SeqStoreAlreadyCreatedException,
        SeqStoreDeleteInProgressException, SeqStoreInternalException
    {
        // This test only makes sense for DedicatedDatabaseInLLMEnv
        Assume.assumeTrue(useLLM && bucketStorageType == BucketStorageType.DedicatedDatabaseInLLMEnv);

        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        AckIdGenerator ackIdGen = new AckIdGenerator(5);

        AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
        StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));

        long bucketSequenceId;

        BDBPersistentManager manager = getManager(useLLM, true, true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            doThrow( new LockNotAvailableException(null, "Forced Failure Exception") ).when(
                    manager.getLongLivedMessagesEnvironment() ).openDatabase( 
                            any( Transaction.class ),
                            any( String.class ),
                            any( DatabaseConfig.class ) );
            
            try {
                manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                fail( "Bucket store creation should have failed");
            } catch( SeqStoreDatabaseException e ) {
                // Success
            }
            
            // The same should happen on retry
            try {
                manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                fail( "Bucket store creation should have failed");
            } catch( SeqStoreDatabaseException e ) {
                // Success
            }
            
            assertEquals(0, manager.getNumOpenBuckets());
            
            // Allow the call to actually work
            doCallRealMethod().when(
                    manager.getLongLivedMessagesEnvironment() ).openDatabase( 
                            any( Transaction.class ),
                            any( String.class ),
                            any( DatabaseConfig.class ) );
            
            
            BucketStore bucketStore = manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
            assertNotNull(bucketStore);

            assertEquals(0, bucketStore.getCount());
            assertNull(bucketStore.getFirstId());
            assertNull(bucketStore.getLastId());

            assertEquals(1, manager.getNumOpenBuckets());

            bucketSequenceId = bucketStore.getSequenceId();

            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertEquals(expectedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            bucketStore.put(storedId1, storedEntry1, true);

            assertEquals(1, bucketStore.getCount());
        } finally {
            manager.close();
            manager = null;
        }
        
        manager = getManager(useLLM, false, false);
        try {
            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertMatchingMetadataCollection(
                    Collections.singleton(expectedMetadata), manager.getBucketMetadataForStore(storeId));
            assertEquals(0, manager.getNumOpenBuckets());

            BucketStore bucketStore = manager.getBucketStore(storeId, bucketId);
            assertEquals(bucketId, bucketStore.getBucketId());

            assertEquals(1, bucketStore.getCount());
            manager.closeBucketStore(storeId, bucketStore);

            manager.deleteBucketStore(storeId, bucketId);

            assertEquals(0, manager.getNumOpenBuckets());
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            // The bucket shouldn't show up after restart if it's been deleted
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }
    }
    
    @Test
    public void testLLMDedicatedBucketCreationFatalFailure()
        throws SeqStoreDatabaseException, IOException, SeqStoreAlreadyCreatedException,
        SeqStoreDeleteInProgressException, SeqStoreInternalException
    {
        // This test only makes sense for DedicatedDatabaseInLLMEnv
        Assume.assumeTrue(useLLM && bucketStorageType == BucketStorageType.DedicatedDatabaseInLLMEnv);

        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        BDBPersistentManager manager = getManager(useLLM, true, true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            doThrow( new EnvironmentFailureException(
                    mock(EnvironmentImpl.class), EnvironmentFailureReason.JAVA_ERROR, "Forced Failure Exception") )
                .when( manager.getLongLivedMessagesEnvironment() )
                .openDatabase( 
                    any( Transaction.class ),
                    any( String.class ),
                    any( DatabaseConfig.class ) );
            
            try {
                manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                fail( "Bucket store creation should have failed");
            } catch( SeqStoreUnrecoverableDatabaseException e ) {
                // Success
            }
        } finally {
            manager.close();
            manager = null;
        }
        
        // Test a read only environment
        SeqStorePersistenceConfig con = getManagerConfig(useLLM, false);
        con.setOpenReadOnly(true);
        manager = new BDBPersistentManager(con.getImmutableConfig(), scheduler);
        try {
            Collection<BucketPersistentMetaData> metadataForStore = manager.getBucketMetadataForStore(storeId);
            assertTrue( metadataForStore.isEmpty() );
        } finally {
            manager.close();
            manager = null;
        }
        
        manager = getManager(useLLM, false, false);
        try {
            Collection<BucketPersistentMetaData> metadataForStore = manager.getBucketMetadataForStore(storeId);
            assertEquals( 1, metadataForStore.size() );
            
            BucketPersistentMetaData bucketMetadata = metadataForStore.iterator().next();
            assertEquals( bucketId, bucketMetadata.getBucketId() );
            assertEquals( getExpectedStorageType(), bucketMetadata.getBucketStorageType() );
            assertEquals( BucketState.CREATING, bucketMetadata.getBucketState() );
            
            assertEquals(0, manager.getNumOpenBuckets());

            BucketStore bucketStore = manager.getBucketStore(storeId, bucketId);
            assertNotNull( bucketStore );
            assertEquals(bucketId, bucketStore.getBucketId());
            
            // Test that the store is usable
            AckIdGenerator ackIdGen = new AckIdGenerator(5);
            AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
            StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));
            bucketStore.put(storedId1, storedEntry1, true);

            assertEquals(1, bucketStore.getCount());

            manager.closeBucketStore(storeId, bucketStore);

            manager.deleteBucketStore(storeId, bucketId);

            assertEquals(0, manager.getNumOpenBuckets());
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            // The bucket shouldn't show up after restart if it's been deleted
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }
    }
    
    @Test
    public void testRollbackTwoNonLLMAfterPartialBucketCreation() throws IOException, SeqStoreException {
        // This test only makes sense for DedicatedDatabaseInLLMEnv
        Assume.assumeTrue(useLLM && bucketStorageType == BucketStorageType.DedicatedDatabaseInLLMEnv);
        
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        AckIdGenerator ackIdGen = new AckIdGenerator(5);

        AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
        StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));

        BDBPersistentManager manager = getManager(true, true, true);
        try {
            assertEquals(0, manager.getNumOpenBuckets());
            
            doThrow( new EnvironmentFailureException(
                    mock(EnvironmentImpl.class), EnvironmentFailureReason.JAVA_ERROR, "Forced Failure Exception") )
                .when( manager.getLongLivedMessagesEnvironment() )
                .openDatabase( 
                    any( Transaction.class ),
                    any( String.class ),
                    any( DatabaseConfig.class ) );

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            try {
                manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                fail( "Bucket store creation should have failed");
            } catch( SeqStoreUnrecoverableDatabaseException e ) {
                // Success
            }
        } finally {
            manager.close();
            manager = null;
        }
        

        // The bucket should still be found and useable even if no new buckets get created with in the llm env
        manager = getManager(false, false);
        try {
            Collection<BucketPersistentMetaData> metadataForStore = manager.getBucketMetadataForStore(storeId);
            assertEquals( 1, metadataForStore.size() );
            
            BucketPersistentMetaData bucketMetadata = metadataForStore.iterator().next();
            assertEquals( bucketId, bucketMetadata.getBucketId() );
            assertEquals( getExpectedStorageType(), bucketMetadata.getBucketStorageType() );
            assertEquals( BucketState.CREATING, bucketMetadata.getBucketState() );
            
            BucketStore bucketStore = manager.getBucketStore(storeId, bucketId);
            assertEquals(bucketId, bucketStore.getBucketId());
            
            bucketStore.put(storedId1, storedEntry1, false);
            
            BucketPersistentMetaData updatedMetadata = new BucketPersistentMetaData(
                    bucketStore.getSequenceId(), bucketId, bucketStore.getBucketStorageType(), 
                    bucketStore.getCount(), storedEntry1.getPayloadSize() + AckIdV3.LENGTH, false);
            boolean result = manager.updateBucketMetadata(storeId, updatedMetadata);
            assertTrue( result );
            
            assertEquals(updatedMetadata, manager.getPersistedBucketData(storeId, bucketId));
            
            assertMatchingMetadataCollection(
                    Collections.singleton(updatedMetadata), manager.getBucketMetadataForStore(storeId));
        } finally {
            manager.close();
            manager = null;
        }
    }
    
    @Test
    public void testLLMDedicatedBucketCreationFailureAfterLLMCommit()
        throws SeqStoreDatabaseException, IOException, SeqStoreAlreadyCreatedException,
        SeqStoreDeleteInProgressException, SeqStoreInternalException
    {
        // This test only makes sense for DedicatedDatabaseInLLMEnv
        Assume.assumeTrue(useLLM && bucketStorageType == BucketStorageType.DedicatedDatabaseInLLMEnv);

        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        AckIdGenerator ackIdGen = new AckIdGenerator(5);

        AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
        StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));

        long bucketSequenceId;
        
        final AtomicBoolean failCreate = new AtomicBoolean(true);

        BDBPersistentManager manager = new BDBPersistentManager(getManagerConfig(useLLM, true).getImmutableConfig(), scheduler) {
            @Override
            BDBBackedBucketStore finishLLMEnvDedicatedBucketCreation(
                String bucketName, BucketPersistentMetaData metadata)
                throws SeqStoreDatabaseException
            {
                BDBBackedBucketStore retval = super.finishLLMEnvDedicatedBucketCreation(bucketName, metadata);
                if( failCreate.get() ) {
                    // Have to close or the store gets leaked
                    retval.close();
                    // Should force an abort
                    throw new SeqStoreDatabaseException("Fake failure exception");
                }
                return retval;
            }
        };
        
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            try {
                manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                fail( "Bucket store creation should have failed");
            } catch( SeqStoreDatabaseException e ) {
                // Success
            }
            
            BucketPersistentMetaData persistedData = manager.getPersistedBucketData(storeId, bucketId);
            assertNotNull( persistedData ); // The partial creation should have been recorded
            bucketSequenceId = persistedData.getBucketSequenceId();
            
            // The same should happen on retry
            try {
                manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                fail( "Bucket store creation should have failed");
            } catch( SeqStoreDatabaseException e ) {
                // Success
            }
            
            // The same should happen on get
            try {
                manager.getBucketStore(storeId, bucketId);
                fail( "Bucket store get should failed if creation partially failed and the retry also failed");
            } catch( SeqStoreDatabaseException e ) {
                // Success
            }
            
            assertEquals(0, manager.getNumOpenBuckets());
            
            // The partial create should have been recorded
            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0,  BucketState.CREATING);
            assertEquals(expectedMetadata, manager.getPersistedBucketData(storeId, bucketId) );
            
            // Allow the call to actually work
            failCreate.set(false);

            // The store should be available through get
            BucketStore bucketStore = manager.getBucketStore(storeId, bucketId);
            assertNotNull(bucketStore);

            assertEquals(0, bucketStore.getCount());
            assertNull(bucketStore.getFirstId());
            assertNull(bucketStore.getLastId());

            assertEquals(1, manager.getNumOpenBuckets());

            bucketSequenceId = bucketStore.getSequenceId();

            expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertEquals(expectedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            bucketStore.put(storedId1, storedEntry1, true);

            assertEquals(1, bucketStore.getCount());
        } catch( RuntimeException e ) {
            throw e;
        } catch( Error e ) {
            throw e;
        } catch( Throwable e ) {
            throw new RuntimeException(e);
        } finally {
            manager.close();
            manager = null;
        }
        
        manager = getManager(useLLM, false, false);
        try {
            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertMatchingMetadataCollection(
                    Collections.singleton(expectedMetadata), manager.getBucketMetadataForStore(storeId));
            assertEquals(0, manager.getNumOpenBuckets());

            BucketStore bucketStore = manager.getBucketStore(storeId, bucketId);
            assertEquals(bucketId, bucketStore.getBucketId());

            assertEquals(1, bucketStore.getCount());
            manager.closeBucketStore(storeId, bucketStore);

            manager.deleteBucketStore(storeId, bucketId);

            assertEquals(0, manager.getNumOpenBuckets());
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }

        manager = getManager(false);
        try {
            // The bucket shouldn't show up after restart if it's been deleted
            assertTrue(manager.getBucketMetadataForStore(storeId).isEmpty());
        } finally {
            manager.close();
            manager = null;
        }
    }
    
    @Test
    public void testLLMParallelCreateAndGet()
        throws SeqStoreDatabaseException, IOException, SeqStoreAlreadyCreatedException,
        SeqStoreDeleteInProgressException, SeqStoreInternalException, InterruptedException
    {
        // This test only makes sense for DedicatedDatabaseInLLMEnv
        Assume.assumeTrue(useLLM && bucketStorageType == BucketStorageType.DedicatedDatabaseInLLMEnv);

        final StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        final AckIdV3 bucketId = new AckIdV3(5, true);
        
        TestExecutorService testExecutorService = new TestExecutorService();
        
        final CountDownLatch getFinishedLatch = new CountDownLatch(1);
        final CountDownLatch createdAtFinishCreation = new CountDownLatch(1);

        final BDBPersistentManager manager = new BDBPersistentManager(getManagerConfig(useLLM, true).getImmutableConfig(), scheduler) {
            private final AtomicInteger finishCallCounter = new AtomicInteger(0);
            
            @Override
            BDBBackedBucketStore finishLLMEnvDedicatedBucketCreation(
                String bucketName, BucketPersistentMetaData metadata)
                throws SeqStoreDatabaseException
            {
                int finishCallCount = finishCallCounter.incrementAndGet();
                assertTrue( finishCallCount <= 2 );
                if( finishCallCount == 1 ) {
                    createdAtFinishCreation.countDown();
                    try {
                        assertTrue( getFinishedLatch.await(1, TimeUnit.SECONDS) );
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                BDBBackedBucketStore retval = super.finishLLMEnvDedicatedBucketCreation(bucketName, metadata);
                if( finishCallCount == 2 ) {
                    getFinishedLatch.countDown();
                }
                return retval;
            }
        };
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);
            
            final List<BucketStore> bucketStores = Collections.synchronizedList( new ArrayList<BucketStore>() );

            testExecutorService.execute( 
                    new Runnable() {
                        @Override
                        public void run() {
                            BucketStore bucketStore;
                            try {
                                bucketStore = manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
                            } catch (SeqStoreDatabaseException e) {
                                throw new RuntimeException(e);
                            }
                            assertNotNull( bucketStore );
                            bucketStores.add ( bucketStore );
                        }
                    } );
            
            testExecutorService.execute( 
                    new Runnable() {
                        @Override
                        public void run() {
                            BucketStore bucketStore;
                            try {
                                assertTrue( createdAtFinishCreation.await(1, TimeUnit.SECONDS) );
                                bucketStore = manager.getBucketStore(storeId, bucketId);
                            } catch (SeqStoreDatabaseException e) {
                                throw new RuntimeException(e);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            assertNotNull( bucketStore );
                            bucketStores.add ( bucketStore );
                        }
                    } );
            
            testExecutorService.shutdownWithin(2, TimeUnit.SECONDS);
            testExecutorService.rethrow();
            
            assertEquals( 2, bucketStores.size() );
            
            AckIdGenerator ackIdGen = new AckIdGenerator(5);
            AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
            StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));
            
            bucketStores.get(0).put(storedId1, storedEntry1, true);

            assertEquals(1, bucketStores.get(0).getCount());
            assertEquals(1, bucketStores.get(1).getCount());
            
            assertEquals( "Test1", bucketStores.get(0).get( storedId1 ).getLogId() );
            assertEquals( "Test1", bucketStores.get(1).get( storedId1 ).getLogId() );
            
            for( BucketStore store : bucketStores ) {
                manager.closeBucketStore(storeId, store);
            }
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testDeletionFailure()
        throws SeqStoreDatabaseException, IOException, SeqStoreAlreadyCreatedException,
        SeqStoreDeleteInProgressException, SeqStoreInternalException
    {
        StoreId storeId = new StoreIdImpl("storeGroup1", null);

        Map<String, AckIdV3> storeAckLevels = new HashMap<String, AckIdV3>();
        storeAckLevels.put("reader", new AckIdV3(0, false));

        AckIdV3 bucketId = new AckIdV3(5, true);

        AckIdGenerator ackIdGen = new AckIdGenerator(5);

        AckIdV3 storedId1 = ackIdGen.getAckId(5, 10);
        StoredEntry<AckIdV3> storedEntry1 = new StoredEntryV3(storedId1, new TestEntry("Test1"));

        long bucketSequenceId;

        final AtomicBoolean failDeletion = new AtomicBoolean(false);
        
        BDBPersistentManager manager = new BDBPersistentManager(getManagerConfig(useLLM, true).getImmutableConfig(), scheduler) {
            @Override
            void deleteDedicatedBucketStore(
                Environment env, Transaction txn, String bucketName, BucketPersistentMetaData metadata)
                throws SeqStoreDatabaseException
            {
                if( failDeletion.get() ) { 
                    throw new SeqStoreDatabaseException("Forced failure");
                }
                super.deleteDedicatedBucketStore(env, txn, bucketName, metadata);
            }
            
            @Override
            void deleteSharedBucketStore(
                String bucketName, Transaction mainEnvTxn, BucketPersistentMetaData metadata)
                throws SeqStoreDatabaseException
            {
                if( failDeletion.get() ) { 
                    throw new SeqStoreDatabaseException("Forced failure");
                }
                super.deleteSharedBucketStore(bucketName, mainEnvTxn, metadata);
            }
        };
        try {
            assertEquals(0, manager.getNumOpenBuckets());

            // Create the store - this isn't needed now but might be at some
            // point
            manager.createStore(storeId, storeAckLevels);

            BucketStore bucketStore = manager.createBucketStore(storeId, bucketId, getExpectedStorageType());
            assertNotNull(bucketStore);

            assertEquals(0, bucketStore.getCount());
            assertNull(bucketStore.getFirstId());
            assertNull(bucketStore.getLastId());

            assertEquals(1, manager.getNumOpenBuckets());

            bucketSequenceId = bucketStore.getSequenceId();

            BucketPersistentMetaData expectedMetadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, getExpectedStorageType(), 0, 0, false);
            assertEquals(expectedMetadata, manager.getPersistedBucketData(storeId, bucketId));

            bucketStore.put(storedId1, storedEntry1, true);

            assertEquals(1, bucketStore.getCount());
            
            manager.closeBucketStore(storeId, bucketStore);
            
            assertEquals(0, manager.getNumOpenBuckets());
            
            failDeletion.set(true);
            
            try {
                manager.deleteBucketStore(storeId, bucketId);
                fail("Bucket store deletion should have failed");
            } catch( SeqStoreDatabaseException e ) {
            }
            
            // Try again - should have the same result
            try {
                manager.deleteBucketStore(storeId, bucketId);
                fail("Bucket store deletion should have failed");
            } catch( SeqStoreDatabaseException e ) {
            }
            
            failDeletion.set(false);
            
            assertTrue( manager.deleteBucketStore(storeId, bucketId) );
            
            assertTrue( manager.getBucketMetadataForStore(storeId).isEmpty() );
        } finally {
            manager.close();
            manager = null;
        }
    }
}
