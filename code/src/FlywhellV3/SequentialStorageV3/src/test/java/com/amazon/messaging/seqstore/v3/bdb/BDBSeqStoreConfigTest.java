package com.amazon.messaging.seqstore.v3.bdb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreConfigTestBase;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.ThrowingConfigProvider;
import com.amazon.messaging.utils.Scheduler;


public class BDBSeqStoreConfigTest extends SeqStoreConfigTestBase {
    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(
        Clock clock, ConfigProvider<BasicInflightInfo> configProvider, Scheduler scheduler)
        throws InvalidConfigException, SeqStoreException, IOException
    {
        return getManager(clock, configProvider, scheduler, true);
    }
    
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(Clock clock,
                                                              ConfigProvider<BasicInflightInfo> configProvider,
                                                              Scheduler scheduler,
                                                              boolean truncate)
            throws InvalidConfigException, SeqStoreException, IOException 
    {
        if( scheduler == null ) {
            scheduler = new Scheduler("BDBSeqStoreConfigTest scheduler", 5);
        }
        
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        if( truncate ) {
            con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        } else {
            con.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
        }
        return BDBStoreManager.createDefaultTestingStoreManager(
                configProvider, con.getImmutableConfig(), clock, scheduler);
    }
    
    @Test
    public void testLoadStoreWithMissingConfig() throws InvalidConfigException, SeqStoreException, IOException {
        final String storeName = "TestStore";
        final StoreId storeId = new StoreIdImpl(storeName);
        
        ThrowingConfigProvider configProvider = new ThrowingConfigProvider();
        
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setRequiredRedundancy(1);
        storeConfig.setMaxMessageLifeTime(50000);
        
        configProvider.putStoreConfig(storeName, storeConfig);
        
        Clock clock = new SettableClock(1);
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider);
        try {
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore( storeId );
            
            assertNotNull( store );
            assertStoreConfigEquals( storeConfig, store.getConfig() );
        } finally {
            manager.close();
            manager = null;
        }
        
        manager = getManager(clock, configProvider, null, false);
        try {
            assertTrue( manager.containsStore( storeId ) );
            
            configProvider.setFailStoreGets( true );
            try {
                manager.getStore( storeId );
                fail( "getStore succeeded without config" );
            } catch( SeqStoreMissingConfigException e ) {
                assertCauseIncluded(e, configProvider);
            }
            
            // Make sure we can still get the store once the config appears
            configProvider.setFailStoreGets(false);
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore( storeId );
            
            assertNotNull( store );
            assertStoreConfigEquals( storeConfig, store.getConfig() );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testLoadReaderWithMissingConfig() throws InvalidConfigException, SeqStoreException, IOException {
        final String storeName = "TestStore";
        final String readerName = "TestReader";
        final StoreId storeId = new StoreIdImpl(storeName);
        
        ThrowingConfigProvider configProvider = new ThrowingConfigProvider();
        
        SeqStoreReaderConfig<BasicInflightInfo> readerConfig = 
            BasicConfigProvider.newBasicInflightReaderConfig();
        readerConfig.setStartingMaxMessageLifetime(5000);
        
        configProvider.putReaderConfig(storeName, readerName, readerConfig);
        
        Clock clock = new SettableClock(1);
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider);
        try {
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore( storeId );
            assertNotNull( store );
            
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = 
                store.createReader( readerName );
            assertNotNull( reader );
            assertReaderConfigEquals(readerConfig, reader.getConfig());
        } finally {
            manager.close();
            manager = null;
        }
        
        manager = getManager(clock, configProvider, null, false);
        try {
            assertTrue( manager.getStoreNames().contains( storeId ) );
            
            configProvider.setFailReaderGets( true );
            try {
                manager.getStore( storeId );
                fail( "Loading a store that has a reader without configuration should fail." );
            } catch( SeqStoreMissingConfigException e ) {
                assertCauseIncluded(e, configProvider);
            }
            
            // Make sure we can still get the store and reader once the config appears
            configProvider.setFailReaderGets( false );
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore( storeId );
            assertNotNull(store);
            
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.getReader( readerName );
            assertNotNull( reader );
            assertReaderConfigEquals(readerConfig, reader.getConfig());
        } finally {
            manager.close();
        }
    }
}
