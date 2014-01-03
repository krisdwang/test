package com.amazon.messaging.seqstore.v3;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;


import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.config.DistributionPolicy;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfigInterface;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfigInterface;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;
import com.amazon.messaging.util.ThrowingConfigProvider;
import com.amazon.messaging.utils.Scheduler;

public abstract class SeqStoreConfigTestBase extends TestCase {
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(
            Clock clock, ConfigProvider<BasicInflightInfo> configProvider)
        throws InvalidConfigException, SeqStoreException, IOException
    {
        return getManager(clock, configProvider, new Scheduler("SeqStoreConfigTestBase scheduler", 5));
    }
    
    protected abstract SeqStoreManagerV3<BasicInflightInfo> getManager(
            Clock clock, ConfigProvider<BasicInflightInfo> configProvider, Scheduler scheduler)
        throws InvalidConfigException, SeqStoreException, IOException;
    
    protected static boolean storeConfigEquals(SeqStoreConfigInterface config1, SeqStoreConfigInterface config2) {
        return beanPropertiesEqual(SeqStoreConfigInterface.class, config1, config2);
    }
    
    protected static boolean readerConfigEquals(SeqStoreReaderConfigInterface<?> config1, 
                                              SeqStoreReaderConfigInterface<?> config2) 
    {
        return beanPropertiesEqual( SeqStoreReaderConfigInterface.class, config1, config2 );
    }

    private static boolean beanPropertiesEqual(Class<?> beanClass, Object obj1, Object obj2) {
        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo( beanClass );
        } catch (IntrospectionException e) {
            throw new RuntimeException("Introspection failed", e);
        }
        
        for( PropertyDescriptor desc : beanInfo.getPropertyDescriptors() ) {
            if( desc.getReadMethod() != null ) {
                Object val1, val2;
                try {
                    val1 = desc.getReadMethod().invoke(obj1);
                    val2 = desc.getReadMethod().invoke(obj2);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Introspection failed", e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("Introspection failed", e);
                }
                
                if( !val1.equals( val2 ) ) return false;
            }
        }
        
        return true;
    }
    
    protected static void assertStoreConfigEquals(SeqStoreConfigInterface config1, SeqStoreConfigInterface config2) {
        assertTrue( config1 + " does not match " + config2, storeConfigEquals(config1, config2));
    }
    
    protected static void assertReaderConfigEquals(SeqStoreReaderConfigInterface<?> config1, 
                                                 SeqStoreReaderConfigInterface<?> config2) {
        assertTrue( config1 + " does not match " + config2, readerConfigEquals(config1, config2));
    }
    
    @Test
    public void testStoreConfigValidation() {
        SeqStoreConfig config = new SeqStoreConfig();
        config.setDesiredRedundancy(1);
        config.setRequiredRedundancy(3);
        assertNotNull( config.validate() );
        
        config = new SeqStoreConfig();
        config.setDistributionPolicy(DistributionPolicy.Span2AreasRequired);
        config.setRequiredRedundancy(1);
        assertNotNull( config.validate() );
        
        config = new SeqStoreConfig();
        config.setDistributionPolicy(DistributionPolicy.Span3AreasRequired);
        config.setRequiredRedundancy(2);
        assertNotNull( config.validate() );
        
        config = new SeqStoreConfig();
        config.setGuaranteedRetentionPeriod(1000);
        config.setMaxMessageLifeTime(100);
        assertNotNull( config.validate() );
        
        config = new SeqStoreConfig();
        config.setGuaranteedRetentionPeriod(-1);
        config.setMaxMessageLifeTime(100);
        assertNotNull( config.validate() );
    }
    
    @Test
    public void testStoreConfigCopy() {
        SeqStoreConfig config = new SeqStoreConfig();
        assertStoreConfigEquals(config, config.clone() );
        assertStoreConfigEquals(config, config.getImmutableConfig());
        
        // Change config so nothing matches the default
        config.setMaxSizeInKBBeforeUsingDedicatedDBBuckets(5);
        config.setDedicatedDBBucketStoreConfig(
                new BucketStoreConfig.Builder()
                    .withMinPeriod(5)
                    .withMaxPeriod(10)
                    .withMaxEnqueueWindow(2)
                    .withMinSize(5)
                    .build());
        config.setSharedDBBucketStoreConfig(
                new BucketStoreConfig.Builder()
                .withMinPeriod(4)
                .withMaxPeriod(15)
                .withMaxEnqueueWindow(3)
                .withMinSize(8)
                .build());
        config.setCleanerPeriod(10000);
        config.setDesiredRedundancy(2);
        config.setRequiredRedundancy(2);
        config.setDistributionPolicy(DistributionPolicy.LocalAreaOnly);
        config.setGuaranteedRetentionPeriod(1000);
        config.setMaxMessageLifeTime(100000);
        config.setMinBucketsToCompress(2);
        
        assertStoreConfigEquals(config, config.clone() );
        assertStoreConfigEquals(config, config.getImmutableConfig());
    }
    
    @Test
    public void testReaderConfigCopy() {
        SeqStoreReaderConfig<BasicInflightInfo> config = BasicConfigProvider.newBasicInflightReaderConfig();
        assertReaderConfigEquals(config, new SeqStoreReaderConfig<BasicInflightInfo>( config ) );
        assertReaderConfigEquals(config, config.getImmutableConfig() );
        
        // Change config so nothing matches the default
        config.setStartingMaxMessageLifetime(2);
        config.setMaxInflightTableMessageCountLimit(5);
        config.setInflightInfoFactory( new BasicInflightInfoFactory(100));
        assertReaderConfigEquals(config, new SeqStoreReaderConfig<BasicInflightInfo>( config ) );
        assertReaderConfigEquals(config, config.getImmutableConfig() );
    }
    
    @Test
    public void testStoreConfigUpdate() throws InvalidConfigException, SeqStoreException, IOException {
        final String storeName = "TestStore";
        
        BasicConfigProvider<BasicInflightInfo> configProvider 
            = BasicConfigProvider.newBasicInflightConfigProvider();
        
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setRequiredRedundancy(1);
        storeConfig.setMaxMessageLifeTime(50000);
        
        configProvider.putStoreConfig(storeName, storeConfig);
        
        Clock clock = new SettableClock(1);
        
        Scheduler scheduler = new Scheduler("testStoreConfigUpdate Scheduler", 5);
        
        boolean success = false;
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider, scheduler);
        try {
            int numScheduledTasks = scheduler.numRemainingTasks();
            
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore(
                    new StoreIdImpl(storeName));
            
            // Cleanup task should have been scheduled
            assertEquals( 1 + numScheduledTasks, scheduler.numRemainingTasks() );
            
            assertNotNull( store );
            assertStoreConfigEquals( storeConfig, store.getConfig() );
            
            storeConfig.setMaxMessageLifeTime(1000);
            assertFalse( "Interal store config is not protected from external changes", 
                    storeConfigEquals( storeConfig, store.getConfig() ) );
            
            configProvider.putStoreConfig(storeName, storeConfig);
            store.updateConfig();
            
            assertStoreConfigEquals( storeConfig, store.getConfig() );
            
            // Cleanup task should have been rescheduled and the old one cancelled
            assertEquals( 1 + numScheduledTasks, scheduler.numRemainingTasks() );
            success = true;
        } finally {
            if( success ) {
                // If everything else worked check that the shutdown is clean
                scheduler.setRequireCleanShutdown(true);
            }
            manager.close();
        }
        
        assertTrue( scheduler.isShutdown() );
    }
    
    @Test
    public void testStoreConfigUpdateFail() throws InvalidConfigException, SeqStoreException, IOException {
        final String storeName = "TestStore";
        
        ThrowingConfigProvider configProvider = new ThrowingConfigProvider();
        
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setRequiredRedundancy(1);
        storeConfig.setMaxMessageLifeTime(50000);
        
        configProvider.putStoreConfig(storeName, storeConfig);
        
        Clock clock = new SettableClock(1);
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider);
        try {
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore(
                    new StoreIdImpl(storeName));
            
            assertNotNull( store );
            assertStoreConfigEquals( storeConfig, store.getConfig() );

            SeqStoreConfig newStoreConfig = storeConfig.clone();
            newStoreConfig.setMaxMessageLifeTime(5000);
            configProvider.putStoreConfig(storeName, newStoreConfig);
            
            configProvider.setFailStoreGets( true );
            try {
                store.updateConfig();
                fail( "Update config succeeded with missing config.");
            } catch( SeqStoreMissingConfigException e ) {
                assertCauseIncluded(e, configProvider);
            }
            assertStoreConfigEquals( storeConfig, store.getConfig() );
            
            configProvider.setFailStoreGets( false );
            store.updateConfig();
            assertStoreConfigEquals( newStoreConfig, store.getConfig() );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testReaderConfigUpdate() throws InvalidConfigException, SeqStoreException, IOException {
        final String storeName = "TestStore";
        final String readerName = "TestReader";
        
        BasicConfigProvider<BasicInflightInfo> configProvider 
            = BasicConfigProvider.newBasicInflightConfigProvider();
        
        SeqStoreReaderConfig<BasicInflightInfo> readerConfig = 
            BasicConfigProvider.newBasicInflightReaderConfig();
        readerConfig.setStartingMaxMessageLifetime(5000);
        
        configProvider.putReaderConfig(storeName, readerName, readerConfig);
        
        Clock clock = new SettableClock(1);
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider);
        try {
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore(
                    new StoreIdImpl(storeName));
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerName);
            
            assertNotNull( reader );
            assertReaderConfigEquals( readerConfig, reader.getConfig() );
            
            readerConfig.setStartingMaxMessageLifetime(1000);
            assertFalse( "Interal reader config is not protected from external changes", 
                    readerConfigEquals(readerConfig, reader.getConfig() ) );
             
            configProvider.putReaderConfig(storeName, readerName, readerConfig);
            reader.updateConfig();
            assertReaderConfigEquals( readerConfig, reader.getConfig() );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testReaderConfigUpdateFail() throws InvalidConfigException, SeqStoreException, IOException {
        final String storeName = "TestStore";
        final String readerName = "TestReader";
        
        ThrowingConfigProvider configProvider = new ThrowingConfigProvider();
        
        SeqStoreReaderConfig<BasicInflightInfo> readerConfig = 
            BasicConfigProvider.newBasicInflightReaderConfig();
        readerConfig.setStartingMaxMessageLifetime(5000);
        
        configProvider.putReaderConfig(storeName, readerName, readerConfig);
        
        Clock clock = new SettableClock(1);
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider);
        try {
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore(
                    new StoreIdImpl(storeName));
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerName);
            
            assertNotNull( reader );
            assertReaderConfigEquals( readerConfig, reader.getConfig() );
            
            SeqStoreReaderConfig<BasicInflightInfo> newReaderConfig = 
                BasicConfigProvider.newBasicInflightReaderConfig();
            newReaderConfig.setStartingMaxMessageLifetime(1000);
            configProvider.putReaderConfig(storeName, readerName, newReaderConfig);
            
            configProvider.setFailReaderGets(true);
            try {
                reader.updateConfig();
                fail( "Update config succeeded with missing config.");
            } catch( SeqStoreMissingConfigException e ) {
                assertCauseIncluded(e, configProvider);
            }
            assertReaderConfigEquals( readerConfig, reader.getConfig() );
            
            configProvider.setFailReaderGets(false);
            reader.updateConfig();
            assertReaderConfigEquals( newReaderConfig, reader.getConfig() );
        } finally {
            manager.close();
        }
    }

    protected void assertCauseIncluded(SeqStoreMissingConfigException exception,
                                     ThrowingConfigProvider configProvider) 
    {
        assertCauseIncluded(exception, exception, configProvider.getLastException() );
    }
    
    private void assertCauseIncluded(Exception root, Throwable current, Exception expectedCause) {
        if( current == expectedCause ) return;
        if( current.getCause() != null ) {
            assertCauseIncluded( root, current.getCause(), expectedCause );
        } else {
            fail("Cause not found in exception tree");
        }
    }
    
    @Test
    public void testStoreConfigMissing() throws InvalidConfigException, SeqStoreException, IOException {
        final String storeName = "TestStore";
        final StoreId storeId = new StoreIdImpl(storeName);
        
        ThrowingConfigProvider configProvider = new ThrowingConfigProvider();
        configProvider.setFailStoreGets(true);
        
        Clock clock = new SettableClock(1);
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider);
        try {
            try {
                manager.createStore( storeId );
                fail( "Created store without config" );
            } catch( SeqStoreMissingConfigException e ) {
                assertCauseIncluded(e, configProvider);
            }
            
            assertFalse( manager.containsStore( storeId ) );
            assertNull( manager.getStore( storeId ) );
            
            // Make sure we can still create a store once the config appears
            configProvider.setFailStoreGets(false);
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore( storeId );
            
            assertNotNull( store );
            assertStoreConfigEquals( SeqStoreConfig.getDefaultConfig(), store.getConfig() );
            assertTrue( manager.getStoreNames().contains( storeId ) );
            assertNotNull( manager.getStore( storeId ) );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testReaderConfigMissing() throws InvalidConfigException, SeqStoreException, IOException {
        final String storeName = "TestStore";
        final String readerName = "TestReader";
        final StoreId storeId = new StoreIdImpl(storeName);
        
        ThrowingConfigProvider configProvider = new ThrowingConfigProvider();
            
        Clock clock = new SettableClock(1);
        
        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(clock, configProvider);
        try {
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore( storeId );

            configProvider.setFailReaderGets(true);
            try {
                store.createReader( readerName );
                fail( "Created reader without config" );
            } catch( SeqStoreMissingConfigException e ) {
                assertCauseIncluded(e, configProvider);
            }
            
            assertFalse( store.getReaderNames().contains( readerName ) );
            assertNull( store.getReader( readerName ) );
            
            // Make sure the reader can still be created once config is available
            configProvider.setFailReaderGets(false);
            SeqStoreReader<AckIdV3, BasicInflightInfo > reader = 
                store.createReader( readerName );
            
            assertNotNull( reader );
            assertTrue( store.getReaderNames().contains( readerName ) );
            assertNotNull( store.getReader( readerName ) );
            
            assertReaderConfigEquals(
                    BasicConfigProvider.newBasicInflightReaderConfig().getImmutableConfig(), 
                    reader.getConfig() );
        } finally {
            manager.close();
        }
    }
}
