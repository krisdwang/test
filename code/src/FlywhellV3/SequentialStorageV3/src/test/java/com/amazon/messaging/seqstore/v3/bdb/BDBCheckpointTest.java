package com.amazon.messaging.seqstore.v3.bdb;

import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.CheckpointTestBase;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;


public class BDBCheckpointTest extends CheckpointTestBase {

    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(
            Clock clock,
            ConfigProvider<BasicInflightInfo> configProvider,
            CheckpointProvider<StoreId, AckIdV3, AckIdV3, BasicInflightInfo> checkpointProvider)
        throws InvalidConfigException, SeqStoreException, IOException 
    {
        return getManager(clock, configProvider, checkpointProvider, true);
    }
    
    private SeqStoreManagerV3<BasicInflightInfo> getManager(Clock clock,
            ConfigProvider<BasicInflightInfo> configProvider,
            CheckpointProvider<StoreId, AckIdV3, AckIdV3, BasicInflightInfo> checkpointProvider,
            boolean truncate)
        throws InvalidConfigException, SeqStoreException, IOException 
    {
        SeqStorePersistenceConfig config = new SeqStorePersistenceConfig();
        if( truncate ) {
            config.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
            config.setTruncateDatabase(true);
        } else {
            config.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
            config.setTruncateDatabase(false);
        }

        AckIdGenerator generator = new AckIdGenerator(clock.getCurrentTime());

        return BDBStoreManager.<BasicInflightInfo> createDefaultTestingStoreManager(
                configProvider, checkpointProvider, config.getImmutableConfig(), generator, clock);
    }
    
    @Test
    public void testRestoreWithCheckpoint() throws InvalidConfigException, SeqStoreException, IOException, InterruptedException {
        final int numMessages = 50;
        final int redeliveryTimeout = 4 * 10;

        final StoreId storeId = new StoreIdImpl("store");
        final String readerId = "Reader1";

        SettableClock clock = new SettableClock();
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        TestCheckpointProvider checkpointProvider = new TestCheckpointProvider();

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(
                clock, configProvider, checkpointProvider, true);

        try {
            SeqStoreReaderConfig<BasicInflightInfo> readerConfig = BasicConfigProvider.newBasicInflightReaderConfig();
            readerConfig.setInflightInfoFactory(new BasicInflightInfoFactory(redeliveryTimeout));
            configProvider.putReaderConfig(storeId.getGroupName(), readerId, readerConfig);
    
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore(storeId);
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
    
            for (int i = 0; i < numMessages; ++i) {
                store.enqueue(new TestEntry("Test" + i), 10, null);
            }
            
            readAndAckMessages(reader, 0, numMessages/4, true, false);
    
            clock.increaseTime(redeliveryTimeout / 2);
            
            readAndAckMessages(reader, numMessages/4, numMessages/2, true, false);
            
            checkpointProvider.storeCheckpoint( storeId, readerId, reader.getCheckpoint());
        } finally {
            manager.close();
        }
        
        manager = getManager(
                clock, configProvider, checkpointProvider, false );
        try { 
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
            
            readAndAckMessages(reader, numMessages/2, numMessages, false, false);
            
            assertNull( reader.dequeue() );
            
            clock.increaseTime(redeliveryTimeout / 2);
            
            readAndAckMessages(reader, 0, numMessages/4, false, true);
            
            assertNull( reader.dequeue() );
    
            clock.increaseTime(redeliveryTimeout / 2);
            
            readAndAckMessages(reader, numMessages/4, numMessages/2, false, true);
    
            assertNull( reader.dequeue() );
        } finally {
            manager.close();
        }
    }
}
