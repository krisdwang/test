package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.bdb.BDBStoreManager;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.DefaultAckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.mapPersistence.MapStoreManager;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.testing.LabelledParameterized;
import com.amazon.messaging.testing.LabelledParameterized.Parameters;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;

@RunWith(LabelledParameterized.class)
public class MetricsTest {
    @Parameters
    public static List<TestParameters> getParams() {
        return Arrays.asList( 
                new TestParameters("BDB", new Object[] { true } ),
                new TestParameters("Map", new Object[] { true } ) );
    }
    
    private boolean useBDB;
    
    public MetricsTest(boolean useBDB) {
        this.useBDB = useBDB;
    }
    
    
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(
            Clock clock, BasicConfigProvider<BasicInflightInfo> configProvider, boolean truncate) 
            throws InvalidConfigException, SeqStoreException, IOException
    {
        if( useBDB ) {
            SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
            if( truncate ) {
                con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
            } else {
                con.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
            }
            
            return BDBStoreManager.createDefaultTestingStoreManager(
                    configProvider, con.getImmutableConfig(),clock);
        } else {
            return new MapStoreManager<BasicInflightInfo>(
                    configProvider, null, 
                    new DefaultAckIdSourceFactory( new AckIdGenerator( clock.getCurrentTime() ), clock), 
                    new StoreSpecificClock(clock) );
        }
    }
    
    
    @Test
    public void testMetrics() throws SeqStoreException, InterruptedException, InvalidConfigException, IOException {
        final String storeName = "Store";
        final String readerName = "Reader";
        final int redeliveryDelay = 10 * 60 * 1000;
        
        final SettableClock clock = new SettableClock();
        final BasicConfigProvider<BasicInflightInfo> configProvider 
            = BasicConfigProvider.newBasicInflightConfigProvider();
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager( clock, configProvider, true);
        try {
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setRequiredRedundancy(2);
            configProvider.putStoreConfig(storeName, storeConfig );
            
            SeqStoreReaderConfig<BasicInflightInfo> readerConfig = 
                new SeqStoreReaderConfig<BasicInflightInfo>(new BasicInflightInfoFactory(redeliveryDelay));
            configProvider.putReaderConfig(storeName, readerName, readerConfig );
            
            SeqStore<AckIdV3, ?> store = manager.createStore(new StoreIdImpl( storeName ) );
            SeqStoreReader<AckIdV3, ?> reader = store.createReader(readerName);
            
            long expectedBytes = 0;
            TestEntry entry1 = new TestEntry( "TestEntry1" );
            store.enqueue( entry1, 500, null);
            expectedBytes += entry1.getPayloadSize() + AckIdV3.LENGTH;
            
            TestEntry entry2 = new TestEntry( "TestEntry2" );
            store.enqueue( entry2, 200, null);
            expectedBytes += entry2.getPayloadSize() + AckIdV3.LENGTH;
            
            clock.increaseTime(1);
            
            SeqStoreMetrics expectedStoreMetrics = new SeqStoreMetrics(
                    new StoredCount( 2, expectedBytes, 1 ), 
                    StoredCount.EmptyStoredCount,
                    1,
                    0.0,
                    0.0);
            
            assertEquals( expectedStoreMetrics, store.getStoreMetrics() );
            assertEquals( expectedStoreMetrics, store.getActiveStoreMetrics() );

            SeqStoreReaderMetrics expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(2, expectedBytes, 1),   // total
                    StoredCount.EmptyStoredCount,           // inflight
                    new StoredCount(2, expectedBytes, 1),   // available
                    StoredCount.EmptyStoredCount,           // delayed
                    1 );                                    // age
            
            assertEquals( expectedReaderMetrics, reader.getStoreBacklogMetrics() );
            assertEquals( new InflightMetrics( 0, 0 ), reader.getInflightMetrics() );
            
            // Read the message
            InflightEntry<AckIdV3, ?> dequeuedEntry = reader.dequeue();
            assertNotNull( dequeuedEntry );
            assertEquals( entry1.getLogId(), dequeuedEntry.getLogId() );
            
            
            assertEquals( new InflightMetrics( 1, 0 ), reader.getInflightMetrics() );
            
            expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(2, expectedBytes, 1),   // total
                    new StoredCount(1, expectedBytes, 1),   // inflight
                    new StoredCount(1, expectedBytes, 1),   // available
                    StoredCount.EmptyStoredCount,           // delayed
                    1 );                                    // age
            
            assertEquals( expectedReaderMetrics, reader.getStoreBacklogMetrics() );
            
            clock.increaseTime( 100 );
            
            expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(2, expectedBytes, 1),   // total
                    new StoredCount(1, expectedBytes, 1),   // inflight
                    new StoredCount(1, expectedBytes, 1),   // available
                    StoredCount.EmptyStoredCount,           // delayed
                    clock.getCurrentTime() - 1 ); // age
            
            assertEquals( expectedReaderMetrics, reader.getStoreBacklogMetrics() );
            
            clock.increaseTime( redeliveryDelay );
            assertEquals( new InflightMetrics( 0, 1 ), reader.getInflightMetrics() );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testDelayedMessageMetrics() throws SeqStoreException, InterruptedException, InvalidConfigException, IOException {
        final long maxPeriodMS = 
                SeqStoreConfig.getDefaultConfig().getDedicatedDBBucketStoreConfig().getMaxPeriod() * 1000;
        final String storeName = "Store";
        final String readerName = "Reader";
        final int redeliveryDelay = 10 * 60 * 1000;
        final long messageDelay1 = 200;
        // Put the second delayed message in a new bucket
        final long messageDelay2 = messageDelay1 + maxPeriodMS + 50; 
        
        final SettableClock clock = new SettableClock( messageDelay1 + 500 );
        final BasicConfigProvider<BasicInflightInfo> configProvider 
            = BasicConfigProvider.newBasicInflightConfigProvider();
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager( clock, configProvider, true);
        try {
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setRequiredRedundancy(2);
            configProvider.putStoreConfig(storeName, storeConfig );
            
            SeqStoreReaderConfig<BasicInflightInfo> readerConfig = 
                new SeqStoreReaderConfig<BasicInflightInfo>(new BasicInflightInfoFactory(redeliveryDelay));
            configProvider.putReaderConfig(storeName, readerName, readerConfig );
            
            SeqStore<AckIdV3, ?> store = manager.createStore(new StoreIdImpl( storeName ) );
            SeqStoreReader<AckIdV3, ?> reader = store.createReader(readerName);
            
            TestEntry entry1 = new TestEntry( "TestEntry1", clock.getCurrentTime() + messageDelay1 );
            store.enqueue( entry1, 200, null);
            long entry1Bytes = entry1.getPayloadSize() + AckIdV3.LENGTH; 
            
            TestEntry entry2 = new TestEntry( "TestEntry2", clock.getCurrentTime() + messageDelay2 );
            store.enqueue( entry2, 200, null);
            long entry2Bytes = entry2.getPayloadSize() + AckIdV3.LENGTH;
            long totalBytes = entry1Bytes + entry2Bytes;
            
            SeqStoreMetrics expectedStoreMetrics = new SeqStoreMetrics(
                    new StoredCount( 2, totalBytes, 2 ), 
                    new StoredCount( 2, totalBytes, 2 ),
                    -messageDelay1,
                    0.0,
                    0.0);
            
            assertEquals( expectedStoreMetrics, store.getStoreMetrics() );
            
            SeqStoreReaderMetrics expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(2, totalBytes, 2),   // total
                    StoredCount.EmptyStoredCount,        // inflight
                    new StoredCount(2, totalBytes, 2),   // unread
                    new StoredCount(2, totalBytes, 2),   // delayed
                    -messageDelay1 );                    // age
            
            assertEquals( expectedReaderMetrics, reader.getStoreBacklogMetrics() );
            
            // Advance the clock to make the first message available
            clock.increaseTime( messageDelay1 );
            
            expectedStoreMetrics = new SeqStoreMetrics(
                    new StoredCount( 2, totalBytes, 2 ), 
                    new StoredCount( 1, entry2Bytes, 1 ),
                    0,
                    0.0,
                    0.0);
            
            assertEquals( expectedStoreMetrics, store.getStoreMetrics() );
            
            expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(2, totalBytes, 2),   // total
                    StoredCount.EmptyStoredCount,        // inflight
                    new StoredCount(2, totalBytes, 2),   // unread
                    new StoredCount(1, entry2Bytes, 1),  // delayed
                    0 );

            assertEquals( expectedReaderMetrics, reader.getStoreBacklogMetrics() );
            
            // Advance the clock so that we're in the same bucket as the next delayed message
            //  but still before it
            clock.increaseTime( maxPeriodMS );
            
            expectedStoreMetrics = new SeqStoreMetrics(
                    new StoredCount( 2, totalBytes, 2 ), 
                    new StoredCount( 1, entry2Bytes, 1 ),
                    maxPeriodMS,
                    0.0,
                    0.0);
            
            assertEquals( expectedStoreMetrics, store.getStoreMetrics() );
            
            expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(2, totalBytes, 2),   // total
                    StoredCount.EmptyStoredCount,        // inflight
                    new StoredCount(2, totalBytes, 2),   // unread
                    new StoredCount(1, entry2Bytes, 1),  // delayed
                    maxPeriodMS );
            
            // Advance the clock so both messages are available
            clock.increaseTime( messageDelay2 - messageDelay1 - maxPeriodMS + 1 );
            
            expectedStoreMetrics = new SeqStoreMetrics(
                    new StoredCount( 2, totalBytes, 2 ), 
                    StoredCount.EmptyStoredCount,
                    messageDelay2 - messageDelay1 + 1,
                    0.0,
                    0.0);
            
            assertEquals( expectedStoreMetrics, store.getStoreMetrics() );
            
            expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(2, totalBytes, 2),   // total
                    StoredCount.EmptyStoredCount,        // inflight
                    new StoredCount(2, totalBytes, 2),   // unread
                    StoredCount.EmptyStoredCount,        // delayed
                    messageDelay2 - messageDelay1 + 1 );     

            assertEquals( expectedReaderMetrics, reader.getStoreBacklogMetrics() );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testMetricsWithIncomleteEnqueues() 
            throws SeqStoreException, InterruptedException, InvalidConfigException, IOException 
    {
        final String storeName = "Store";
        final String readerName = "Reader";
        final int redeliveryDelay = 10 * 60 * 1000;
        
        final SettableClock clock = new SettableClock();
        final BasicConfigProvider<BasicInflightInfo> configProvider 
            = BasicConfigProvider.newBasicInflightConfigProvider();
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager( clock, configProvider, true);
        try {
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setRequiredRedundancy(2);
            configProvider.putStoreConfig(storeName, storeConfig );
            
            SeqStoreReaderConfig<BasicInflightInfo> readerConfig = 
                new SeqStoreReaderConfig<BasicInflightInfo>(new BasicInflightInfoFactory(redeliveryDelay));
            configProvider.putReaderConfig(storeName, readerName, readerConfig );
            
            SeqStoreInternalInterface<?> store = manager.createStore(new StoreIdImpl( storeName ) );
            SeqStoreReader<AckIdV3, ?> reader = store.createReader(readerName);
            
            long expectedBytes = 0;
            TestEntry entry1 = new TestEntry( "TestEntry1" );
            store.enqueue( entry1, 500, null);
            expectedBytes += entry1.getPayloadSize() + AckIdV3.LENGTH;
            
            AckIdV3 incompleteEntryAckId = store.getAckIdForEnqueue(0);
            
            TestEntry entry2 = new TestEntry( "TestEntry2" );
            store.enqueue( entry2, 200, null);
            expectedBytes += entry2.getPayloadSize() + AckIdV3.LENGTH;
            
            clock.increaseTime(1);
            
            SeqStoreMetrics expectedStoreMetrics = new SeqStoreMetrics(
                    new StoredCount( 2, expectedBytes, 1 ), 
                    StoredCount.EmptyStoredCount,
                    1,
                    0.0,
                    0.0);
            
            assertEquals( expectedStoreMetrics, store.getStoreMetrics() );
            assertEquals( expectedStoreMetrics, store.getActiveStoreMetrics() );

            SeqStoreReaderMetrics expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(2, expectedBytes, 1),   // total
                    StoredCount.EmptyStoredCount,           // inflight
                    new StoredCount(2, expectedBytes, 1),   // available
                    StoredCount.EmptyStoredCount,           // delayed
                    1 );                                    // age
            
            assertEquals( expectedReaderMetrics, reader.getStoreBacklogMetrics() );
            assertEquals( new InflightMetrics( 0, 0 ), reader.getInflightMetrics() );
            
            TestEntry entry3 = new TestEntry( "TestEntry3" );
            store.enqueue( entry3, 200, null);
            expectedBytes += entry3.getPayloadSize() + AckIdV3.LENGTH;
            store.enqueueFinished(incompleteEntryAckId);
            
            expectedStoreMetrics = new SeqStoreMetrics(
                    new StoredCount( 3, expectedBytes, 1 ), 
                    StoredCount.EmptyStoredCount,
                    1,
                    0.0,
                    0.0);
            
            assertEquals( expectedStoreMetrics, store.getStoreMetrics() );
            assertEquals( expectedStoreMetrics, store.getActiveStoreMetrics() );

            expectedReaderMetrics = new SeqStoreReaderMetrics(
                    new StoredCount(3, expectedBytes, 1),   // total
                    StoredCount.EmptyStoredCount,           // inflight
                    new StoredCount(3, expectedBytes, 1),   // available
                    StoredCount.EmptyStoredCount,           // delayed
                    1 );                                    // age
            
            assertEquals( expectedReaderMetrics, reader.getStoreBacklogMetrics() );
            assertEquals( new InflightMetrics( 0, 0 ), reader.getInflightMetrics() );
        } finally {
            manager.close();
        }
    }
}
