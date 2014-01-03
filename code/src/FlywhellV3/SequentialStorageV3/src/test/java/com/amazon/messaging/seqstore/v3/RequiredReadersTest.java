package com.amazon.messaging.seqstore.v3;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.mapPersistence.MapStoreManager;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

import static org.junit.Assert.*; 

public class RequiredReadersTest extends TestCase {
    @Test
    public void testNoRequiredReaders() throws SeqStoreException {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        SettableClock clock = new SettableClock();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock );
        try {
            StoreId storeId = new StoreIdImpl("Store", "1");
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(storeId);
            assertEquals( Collections.emptySet(), store.getReaderNames() );
            assertNull( store.getReader("Reader1" ) );
            assertNotNull( store.createReader("Reader1") );
            assertEquals( Collections.singleton("Reader1"), store.getReaderNames() );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testOneRequiredReader() throws SeqStoreException {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        SettableClock clock = new SettableClock();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock );
        try {
            Set<String> requiredReaders = Collections.singleton("Reader1");
            StoreId storeId = new StoreIdImpl("Store", "1");
            configProvider.setRequiredReaders(storeId.getGroupName(), requiredReaders );
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(storeId);
            assertEquals( requiredReaders, store.getReaderNames() );
            assertNotNull( store.getReader("Reader1" ) );
            assertNull( store.getReader("Reader2" ) );
            assertNotNull( store.createReader("Reader2") );
            assertEquals( new HashSet<String>( Arrays.asList( "Reader1", "Reader2") ), store.getReaderNames() );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testMultipleRequiredReaders() throws SeqStoreException {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        SettableClock clock = new SettableClock();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock );
        try {
            Set<String> requiredReaders = new HashSet<String>( Arrays.asList( "Reader1", "Reader2", "Reader3") );
            StoreId storeId = new StoreIdImpl("Store", "1");
            configProvider.setRequiredReaders(storeId.getGroupName(), requiredReaders );
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(storeId);
            assertEquals( requiredReaders, store.getReaderNames() );
            for( String reader : requiredReaders ) {
                assertNotNull( store.getReader( reader ) );
            }
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testPreexistingReadersNoRequired() throws SeqStoreException {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        SettableClock clock = new SettableClock();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock );
        try {
            StoreId storeId = new StoreIdImpl("Store", "1");
            Map<String, AckIdV3> ackLevels = new HashMap<String, AckIdV3>();
            ackLevels.put( "Reader1", new AckIdV3( 0, false ) );
            manager.getPersistenceManager().createStore( storeId, ackLevels);
            
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(storeId);
            assertEquals( Collections.singleton("Reader1"), store.getReaderNames() );
            assertNotNull( store.getReader("Reader1" ) );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testPreexistingReadersMultipleRequiredNoOverlap() throws SeqStoreException {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        SettableClock clock = new SettableClock();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock );
        try {
            StoreId storeId = new StoreIdImpl("Store", "1");
            Map<String, AckIdV3> ackLevels = new HashMap<String, AckIdV3>();
            ackLevels.put( "Reader1", new AckIdV3( 0, false ) );
            manager.getPersistenceManager().createStore( storeId, ackLevels);
            
            Set<String> requiredReaders = new HashSet<String>( Arrays.asList( "Reader2", "Reader3") );
            configProvider.setRequiredReaders(storeId.getGroupName(), requiredReaders );
            
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(storeId);
            Set<String> expectedReaders = new HashSet<String>( requiredReaders );
            expectedReaders.add( "Reader1" );
            assertEquals( expectedReaders, store.getReaderNames() );
            for( String reader : expectedReaders ) {
                assertNotNull( store.getReader( reader ) );
            }
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testPreexistingReadersMultipleRequiredWithOverlap() throws SeqStoreException {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        SettableClock clock = new SettableClock();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock );
        try {
            StoreId storeId = new StoreIdImpl("Store", "1");
            Map<String, AckIdV3> ackLevels = new HashMap<String, AckIdV3>();
            ackLevels.put( "Reader1", new AckIdV3( 0, false ) );
            manager.getPersistenceManager().createStore( storeId, ackLevels);
            
            Set<String> requiredReaders = new HashSet<String>( Arrays.asList( "Reader1", "Reader2", "Reader3") );
            configProvider.setRequiredReaders(storeId.getGroupName(), requiredReaders );
            
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(storeId);
            assertEquals( requiredReaders, store.getReaderNames() );
            for( String reader : requiredReaders ) {
                assertNotNull( store.getReader( reader ) );
            }
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testAddReadersOnUpdateConfig() throws SeqStoreException {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        SettableClock clock = new SettableClock();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock );
        try {
            StoreId storeId = new StoreIdImpl("Store", "1");
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(storeId);
            assertEquals( Collections.emptySet(), store.getReaderNames() );
            assertNull( store.getReader("Reader1" ) );
            
            Set<String> requiredReaders = Collections.singleton("Reader1");
            configProvider.setRequiredReaders(storeId.getGroupName(), requiredReaders );
            
            store.updateConfig();
            
            assertEquals( requiredReaders, store.getReaderNames() );
            assertNotNull( store.getReader("Reader1" ) );
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testDontRemoveReadersOnUpdateConfig() throws SeqStoreException {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        SettableClock clock = new SettableClock();
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock );
        try {
            Set<String> requiredReaders = Collections.singleton("Reader1");
            StoreId storeId = new StoreIdImpl("Store", "1");
            configProvider.setRequiredReaders(storeId.getGroupName(), requiredReaders );
            SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(storeId);
            assertEquals( requiredReaders, store.getReaderNames() );
            assertNotNull( store.getReader("Reader1" ) );
            
            configProvider.setRequiredReaders(storeId.getGroupName(), Collections.<String>emptySet() );
            store.updateConfig();
            
            assertEquals( requiredReaders, store.getReaderNames() );
            assertNotNull( store.getReader("Reader1" ) );
        } finally {
            manager.close();
        }
    }
}
