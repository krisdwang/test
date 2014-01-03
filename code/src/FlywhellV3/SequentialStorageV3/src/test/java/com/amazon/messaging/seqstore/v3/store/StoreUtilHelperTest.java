package com.amazon.messaging.seqstore.v3.store;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.AckId;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.bdb.BDBStoreManager;
import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;


public class StoreUtilHelperTest extends TestCase {
    private static final long BUCKET_INTERVAL_MS = 1000;
    private static final long BASE_TIME = 5100;
    private static final String NO_CLEANUP_GROUP = "NoCleanup";
    private static final String NO_MAX_30S_GUARANTEED_GROUP = "NoMax30sGuaranteed";
    private static final String ONE_MIN_MAX_30S_GUARANTEED_GROUP = "1MinMax30sGuaranteed";
    private static final String ONE_MIN_MAX_NO_GUARANTEED_GROUP = "1MinMaxNoGuaranteed";
    private static final String NO_MAX_OR_GUARANTEED_GROUP = "noMaxOrGuaranteed";
    
    private static final Set<String> GROUPS_FOR_CLEANUP = new HashSet<String>(
            Arrays.asList( 
                    NO_CLEANUP_GROUP, NO_MAX_30S_GUARANTEED_GROUP, 
                    ONE_MIN_MAX_30S_GUARANTEED_GROUP, 
                    ONE_MIN_MAX_NO_GUARANTEED_GROUP, 
                    NO_MAX_OR_GUARANTEED_GROUP ) );
    
    private static final List<Long> CLEANUP_TEST_MESSAGE_TIMES = 
            Arrays.asList(
                    BASE_TIME,
                    BASE_TIME + 5,
                    BASE_TIME + BUCKET_INTERVAL_MS + 1,
                    BASE_TIME + BUCKET_INTERVAL_MS + 6,
                    BASE_TIME + 31 * BUCKET_INTERVAL_MS,
                    BASE_TIME + 31 * BUCKET_INTERVAL_MS + 5,
                    BASE_TIME + 62 * BUCKET_INTERVAL_MS,
                    BASE_TIME + 62 * BUCKET_INTERVAL_MS + 5);
    
    private static final long LAST_CLEANUP_TEST_MESSAGE_TIME = 
            CLEANUP_TEST_MESSAGE_TIMES.get(CLEANUP_TEST_MESSAGE_TIMES.size()-1);
            
    private SeqStoreConfig getConfigForCleanup(long maximum, long guaranteed) {
        
        BucketStoreConfig bucketStoreConfiguration 
            = new BucketStoreConfig.Builder()
                .withMaxEnqueueWindow(0)
                .withMaxPeriod((int) (BUCKET_INTERVAL_MS/1000) )
                .withMinPeriod((int) (BUCKET_INTERVAL_MS/1000) )
                .withMinSize(0)
                .build();
        
        SeqStoreConfig config = new SeqStoreConfig();
        config.setMaxMessageLifeTime(maximum);
        config.setGuaranteedRetentionPeriod(guaranteed);
        config.setSharedDBBucketStoreConfig(bucketStoreConfiguration);
        config.setCleanerPeriod(1000000000);
        return config;
    }
    
    private ConfigProvider<BasicInflightInfo> getConfigProviderForCleanupTests() {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        
        configProvider.putStoreConfig(NO_MAX_OR_GUARANTEED_GROUP, getConfigForCleanup(-1,0));
        configProvider.putStoreConfig(ONE_MIN_MAX_NO_GUARANTEED_GROUP, getConfigForCleanup(60*1000,0));
        configProvider.putStoreConfig(ONE_MIN_MAX_30S_GUARANTEED_GROUP, getConfigForCleanup(60*1000,30*1000));
        configProvider.putStoreConfig(NO_MAX_30S_GUARANTEED_GROUP, getConfigForCleanup(-1,30*1000));
        configProvider.putStoreConfig(NO_CLEANUP_GROUP, getConfigForCleanup(-1,-1));
        
        return configProvider;
    }
    
    private void enqueue( 
        Map<AckIdV3, String> enqueuedEntries,
        SeqStoreInternalInterface<BasicInflightInfo> store, int id, long time) 
            throws SeqStoreException, InterruptedException 
    {
        String logId = "Entry" + id;
        AckIdV3 ackId = store.getAckIdForEnqueue(time);
        try {
            store.enqueue(ackId, new TestEntry(logId, time), 0, true, null);
        } finally {
            store.enqueueFinished(ackId);
        }
        enqueuedEntries.put( ackId, logId );
    }
    
    private NavigableMap<AckIdV3, String> populateStoreForCleanup( BDBStoreManager<BasicInflightInfo> storeManager, StoreId storeId ) 
            throws SeqStoreException, InterruptedException 
    {
        NavigableMap<AckIdV3, String> entryMap = new TreeMap<AckIdV3, String>();
        SeqStoreInternalInterface<BasicInflightInfo> store = storeManager.createStore(storeId);
        
        int id = 1;
        for( long time : CLEANUP_TEST_MESSAGE_TIMES ) {
            enqueue( entryMap, store, id++, time);
        }
        
        return entryMap;
    }
    
    private Map<String, NavigableMap<AckIdV3, String>> populateStoresForCleanup(BDBStoreManager<BasicInflightInfo> storeManager)
            throws SeqStoreException, InterruptedException 
    {
        Map<String, NavigableMap<AckIdV3, String>> retval = new HashMap<String, NavigableMap<AckIdV3,String>>();
        for( String group : GROUPS_FOR_CLEANUP ) {
            retval.put( group, populateStoreForCleanup(storeManager, new StoreIdImpl( group, null ) ) );
        }
        return retval;
    }
    
    private BDBStoreManager<BasicInflightInfo> getStoreManager(
            Clock clock, ConfigProvider<BasicInflightInfo> configProvider, boolean truncate ) 
                    throws IOException, InvalidConfigException, SeqStoreException
    {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        if( truncate ) {
            con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        } else {
            con.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
        }
        return BDBStoreManager.createDefaultTestingStoreManager(
                configProvider, con.getImmutableConfig(),clock);
    }
    
    private StoreUtilHelper getHelper(boolean readonly, ConfigProvider<BasicInflightInfo> configProvider, Clock clock)
            throws SeqStoreException 
    {
        File path = SeqStoreTestUtils.getBDBDir(getClass().getSimpleName());
        return new StoreUtilHelper(path, readonly, clock, configProvider); 
    }
    
    /**
     * Validate StoreUtil cleanup works
     * @param entries a map from store name to the map of entries enqueued into that store
     * @param minReaderLevel the expected minimum ack level across all readers before
     *   cleanup is run. This does not have to be exact but no messages should be available
     *   between this and the actual minimum ack level of the readers
     * @param expectedReaders the expected readers for the store
     * @param configProvider the config provider
     */
    private void validateCleanup(
        Map<String, NavigableMap<AckIdV3, String>> entries, AckIdV3 minReaderLevel, 
        Set<String> expectedReaders, ConfigProvider<BasicInflightInfo> configProvider)
                throws SeqStoreException, ConfigUnavailableException, IOException 
    {
        SettableClock clock = new SettableClock(LAST_CLEANUP_TEST_MESSAGE_TIME + 1);
        StoreUtilHelper helper = getHelper(false, configProvider, clock);
        try {
            Map< String, Map< String, AckIdV3 > > initialReaderLevelsPerGroup = new HashMap< String, Map< String, AckIdV3 > >();
            Map< String, AckIdV3 > initialAckLevels = new HashMap<String, AckIdV3>();
            
            for( String group : GROUPS_FOR_CLEANUP ) {
                NavigableMap<AckIdV3, String> storeEntries = entries.get(group);
                
                assertEquals( expectedReaders, helper.getReaderNames(group) );
                
                AckIdV3 initialStoreAckLevel = null;
                Map<String, AckIdV3> initialReaderLevels = helper.getReaderLevels(group);
                for( AckIdV3 level : initialReaderLevels.values() ) {
                    initialStoreAckLevel = AckIdV3.min(initialStoreAckLevel, level);
                }
                
                if( initialStoreAckLevel == null ) initialStoreAckLevel = new AckIdV3( storeEntries.lastKey(), true );
                
                if( initialStoreAckLevel.compareTo( minReaderLevel ) >= 0 ) {
                    assertEquals( Collections.emptyMap(), storeEntries.subMap( minReaderLevel, false, initialStoreAckLevel, false ) );
                } else {
                    assertEquals( Collections.emptyMap(), storeEntries.subMap( initialStoreAckLevel, false, minReaderLevel, false ) );
                }
                
                initialReaderLevelsPerGroup.put( group, initialReaderLevels );
                initialAckLevels.put( group, initialStoreAckLevel );
            }
            
            // Do the cleanup
            helper.cleanup();
            
            for( String group : GROUPS_FOR_CLEANUP ) {
                NavigableMap<AckIdV3, String> storeEntries = entries.get(group);
                
                // The set of readers shouldn't have changed
                assertEquals( expectedReaders, helper.getReaderNames(group) );
                
                Map<String, AckIdV3> initialReaderLevels = initialReaderLevelsPerGroup.get(group);
                AckIdV3 initialStoreAckLevel = initialAckLevels.get(group);
                
                SeqStoreImmutableConfig config = configProvider.getStoreConfig(group);
                NavigableMap<AckIdV3, String> expectedEntries;
                
                AckIdV3 storeExpectedMin;
                if( config.getGuaranteedRetentionPeriod() < 0 ) {
                    storeExpectedMin = new AckIdV3(0, false);
                } else if( config.getMaxMessageLifeTime() == -1 ) {
                    storeExpectedMin = 
                            AckIdV3.min( 
                                    initialStoreAckLevel, 
                                    new AckIdV3( clock.getCurrentTime() - config.getGuaranteedRetentionPeriod(), false ));
                } else {
                    storeExpectedMin = 
                            AckIdV3.max( 
                                    initialStoreAckLevel, 
                                    new AckIdV3( clock.getCurrentTime() - config.getMaxMessageLifeTime(), false ));
                    
                    storeExpectedMin = 
                            AckIdV3.min( 
                                    storeExpectedMin, 
                                    new AckIdV3( clock.getCurrentTime() - config.getGuaranteedRetentionPeriod(), false ));
                }
                
                expectedEntries = storeEntries.tailMap( storeExpectedMin, true );
                final Iterator<Map.Entry<AckIdV3, String>> entryItr = expectedEntries.entrySet().iterator();
                final int visited[] = {0};
                
                helper.visitMessages(group, 0, Long.MAX_VALUE, Long.MAX_VALUE, 
                        new StoreUtilHelper.StoreUtilsMessageVisitor() {
                            @Override
                            public void visitMessage(StoredEntry<? extends AckId> entry) throws IOException {
                                assertTrue( entryItr.hasNext() );
                                Map.Entry<AckIdV3, String> expectedEntry = entryItr.next();
                                assertEquals( expectedEntry.getKey(), entry.getAckId() );
                                assertEquals( expectedEntry.getValue(), entry.getLogId() );
                                visited[0]++;
                            }
                });
                // Check that all entries are visited
                assertEquals( expectedEntries.size(), visited[0] );

                for( Map.Entry<String, AckIdV3> entry : helper.getReaderLevels(group).entrySet() ) {
                    AckIdV3 oldLevel = initialReaderLevels.get(entry.getKey());
                    if( oldLevel.compareTo(storeExpectedMin) < 0 ) {
                        assertEquals( storeExpectedMin, entry.getValue() );
                    } else {
                        assertEquals( oldLevel, entry.getValue() );
                    }
                }
            }
        } finally {
            helper.close();
        }
    }
    
    @Test
    public void testCleanupWithReaderAtStart() 
            throws InvalidConfigException, IOException, SeqStoreException, InterruptedException, ConfigUnavailableException 
    {
        Map<String, NavigableMap<AckIdV3, String>> entries;
        SettableClock clock = new SettableClock( BASE_TIME );
        ConfigProvider<BasicInflightInfo> configProvider = getConfigProviderForCleanupTests();
        BDBStoreManager<BasicInflightInfo> storeManager = getStoreManager(clock, configProvider, true);
        try {
            for( String group : GROUPS_FOR_CLEANUP ) {
                SeqStore<?,?> store = storeManager.createStore( new StoreIdImpl(group, null) );
                store.createReader("reader1");
                store.createReader("reader2");
            }
            entries = populateStoresForCleanup(storeManager);
            
            clock.setCurrentTime(LAST_CLEANUP_TEST_MESSAGE_TIME);
            for( String group : GROUPS_FOR_CLEANUP ) {
                SeqStore<AckIdV3,?> store = storeManager.getStore( new StoreIdImpl(group, null) );
                assertNotNull( store );
                SeqStoreReader<AckIdV3, ?> reader = store.getReader("reader1");
                assertNotNull(reader);
                // Ack the first 3 of entries on reader1 
                for( int i = 0; i < 3; ++i ) {
                    StoredEntry<AckIdV3> entry = reader.dequeue();
                    assertNotNull( entry );
                    reader.ack(entry.getAckId());
                }
            }
        } finally {
            storeManager.close();
        }
        
        validateCleanup(entries, new AckIdV3( 0, false ), new HashSet<String>(Arrays.asList("reader1", "reader2") ), configProvider);
    }
    
    @Test
    public void testCleanupWithReadersAtEnd() 
            throws InvalidConfigException, IOException, SeqStoreException, InterruptedException, ConfigUnavailableException 
    {
        Map<String, NavigableMap<AckIdV3, String>> entries;
        SettableClock clock = new SettableClock( BASE_TIME );
        ConfigProvider<BasicInflightInfo> configProvider = getConfigProviderForCleanupTests();
        BDBStoreManager<BasicInflightInfo> storeManager = getStoreManager(clock, configProvider, true);
        try {
            for( String group : GROUPS_FOR_CLEANUP ) {
                SeqStore<?,?> store = storeManager.createStore( new StoreIdImpl(group, null) );
                store.createReader("reader1");
                store.createReader("reader2");
            }
            entries = populateStoresForCleanup(storeManager);
            
            clock.setCurrentTime(LAST_CLEANUP_TEST_MESSAGE_TIME);
            for( String group : GROUPS_FOR_CLEANUP ) {
                SeqStore<AckIdV3,?> store = storeManager.getStore( new StoreIdImpl(group, null) );
                assertNotNull( store );
                for( String readerName : Arrays.asList( "reader1", "reader2" ) ) {
                    SeqStoreReader<AckIdV3, ?> reader = store.getReader(readerName);
                    assertNotNull(reader);
                    StoredEntry<AckIdV3> entry;
                    while( ( entry = reader.dequeue() ) != null ) {
                        reader.ack(entry.getAckId());
                    }
                }
            }
        } finally {
            storeManager.close();
        }
        
        validateCleanup(
                entries, new AckIdV3(LAST_CLEANUP_TEST_MESSAGE_TIME, true ), 
                new HashSet<String>(Arrays.asList("reader1", "reader2") ), configProvider);
    }
    
    @Test
    public void testCleanupWithReadersInMiddle() 
            throws InvalidConfigException, IOException, SeqStoreException, InterruptedException, ConfigUnavailableException 
    {
        Map<String, NavigableMap<AckIdV3, String>> entries;
        SettableClock clock = new SettableClock( BASE_TIME );
        ConfigProvider<BasicInflightInfo> configProvider = getConfigProviderForCleanupTests();
        BDBStoreManager<BasicInflightInfo> storeManager = getStoreManager(clock, configProvider, true);
        try {
            for( String group : GROUPS_FOR_CLEANUP ) {
                SeqStore<?,?> store = storeManager.createStore( new StoreIdImpl(group, null) );
                store.createReader("reader1");
                store.createReader("reader2");
            }
            entries = populateStoresForCleanup(storeManager);
            
            clock.setCurrentTime(LAST_CLEANUP_TEST_MESSAGE_TIME);
            for( String group : GROUPS_FOR_CLEANUP ) {
                SeqStore<AckIdV3,?> store = storeManager.getStore( new StoreIdImpl(group, null) );
                assertNotNull( store );
                for( String readerName : Arrays.asList( "reader1", "reader2" ) ) {
                    SeqStoreReader<AckIdV3, ?> reader = store.getReader(readerName);
                    assertNotNull(reader);
                    // Ack the first 3 of entries on the reader 
                    for( int i = 0; i < 3; ++i ) {
                        StoredEntry<AckIdV3> entry = reader.dequeue();
                        assertNotNull( entry );
                        reader.ack(entry.getAckId());
                    }
                }
            }
        } finally {
            storeManager.close();
        }
        
        long thirdTime = CLEANUP_TEST_MESSAGE_TIMES.get(2);
        
        validateCleanup(
                entries, new AckIdV3(thirdTime, true), 
                new HashSet<String>(Arrays.asList("reader1", "reader2") ), configProvider);
    }
    
    @Test
    public void testCleanupNoReader() 
            throws InvalidConfigException, IOException, SeqStoreException, InterruptedException, ConfigUnavailableException 
    {
        Map<String, NavigableMap<AckIdV3, String>> entries;
        SettableClock clock = new SettableClock( BASE_TIME );
        ConfigProvider<BasicInflightInfo> configProvider = getConfigProviderForCleanupTests();
        BDBStoreManager<BasicInflightInfo> storeManager = getStoreManager(clock, configProvider, true);
        try {
            entries = populateStoresForCleanup(storeManager);
            clock.setCurrentTime(LAST_CLEANUP_TEST_MESSAGE_TIME);
        } finally {
            storeManager.close();
        }
        
        validateCleanup(
                entries, new AckIdV3(LAST_CLEANUP_TEST_MESSAGE_TIME, true ), Collections.<String>emptySet(), configProvider);
    }
}
