package com.amazon.messaging.seqstore.v3.internal.jmx;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.mapPersistence.MapStoreManager;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

/**
 * A test class that tests that MBean provided by the SeqStore works. This test class only tests
 * the mbean it does not test that all the metrics it exposes are calculated correctly.
 */
public class SeqStoreViewMBeanTest {
    @Test
    public void testMBean() throws SeqStoreException, IOException, InterruptedException {
        SettableClock clock = new SettableClock();
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock);
        
        try {
            final StoreIdImpl destinationId = new StoreIdImpl("destination", "1");
            
            // Ensure that the store config is what we expect it to be
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setGuaranteedRetentionPeriod(0);
            storeConfig.setMaxMessageLifeTime(-1);
            configProvider.putStoreConfig("destination", storeConfig);
    
            // Create store and readers
            manager.createStore(destinationId);
            final SeqStoreInternalInterface<BasicInflightInfo> store = manager.getStore(destinationId);
            
            Object mbean = store.getMBean();
            assertNotNull( mbean );
            assertTrue( mbean instanceof SeqStoreViewMBean );
            SeqStoreViewMBean viewMBean = ( SeqStoreViewMBean ) mbean;
            
            assertEquals( 0, viewMBean.getAvailableMessageCount() );
            assertEquals( 0, viewMBean.getDelayedMessageCount() );
            assertEquals( 0, viewMBean.getEnqueueCount() );
            assertEquals( 0, viewMBean.getMessageCount() );
            assertEquals( 0, viewMBean.getNumberOfBuckets() );
            assertEquals( 0, viewMBean.getOldestMessageLogicalAge() );
            assertEquals( 0, viewMBean.getStoredSize() );
            
            TestEntry entry = new TestEntry("TestPayloay");
            store.enqueue( entry, 0, null );
            
            clock.increaseTime(1);
            
            assertEquals( 1, viewMBean.getAvailableMessageCount() );
            assertEquals( 0, viewMBean.getDelayedMessageCount() );
            assertEquals( 1, viewMBean.getEnqueueCount() );
            assertEquals( 1, viewMBean.getMessageCount() );
            assertEquals( 1, viewMBean.getNumberOfBuckets() );
            assertEquals( 1, viewMBean.getOldestMessageLogicalAge() );
            assertEquals( entry.getPayloadSize() + AckIdV3.LENGTH, viewMBean.getStoredSize() );
        } finally {
            manager.close();
        }
    }
}
