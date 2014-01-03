package com.amazon.messaging.seqstore.v3.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;


public abstract class StoreDeletionTestBase extends TestCase {
    protected abstract SeqStoreManagerV3<BasicInflightInfo> createManager( 
            BasicConfigProvider<BasicInflightInfo> configProvider, Clock clock, boolean truncate) 
            throws IOException, InvalidConfigException, SeqStoreException;
    
    @Test
    public void testRemoveReader() throws SeqStoreException, InterruptedException, ExecutionException, InvalidConfigException, IOException {
        TestExecutorService executorService = new TestExecutorService( "testRemoveReaderNoDequeue()" );
        
        final BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        final StoreId storeId = new StoreIdImpl("StoreId");
        final String readerId = "reader";
        
        final SeqStoreManager<AckIdV3, BasicInflightInfo> manager = createManager(
                configProvider, new AlwaysIncreasingClock(), false);
        
        try {
            // Ensure that the message isn't removed in time there is no reader
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setGuaranteedRetentionPeriod(10000); 
            storeConfig.setMaxMessageLifeTime(-1);
            configProvider.putStoreConfig(storeId.getGroupName(), storeConfig);
            
            manager.createStore(storeId);
            final SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
            final SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
            assertEquals( Collections.singleton( readerId ), store.getReaderNames() );
            
            TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
            store.enqueue(msgIn1, -1, null);
            
            store.removeReader(readerId);
            
            assertEquals( Collections.emptySet(), store.getReaderNames() );
            
            try {
                reader.getInflightMetrics();
                fail( "Operation on removed reader did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            SeqStoreReader<AckIdV3, BasicInflightInfo> newReader = store.createReader(readerId);
            
            try {
                reader.getInflightMetrics();
                fail( "Operation on removed reader did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            InflightEntry<AckIdV3, BasicInflightInfo> entry = newReader.dequeue();
            assertNotNull(newReader);
            assertEquals(msgIn1.getLogId(), entry.getLogId());
            
            executorService.shutdownWithin(100, TimeUnit.MILLISECONDS);
            executorService.rethrow();
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testRemoveStore() throws SeqStoreException, InterruptedException, ExecutionException, TimeoutException, InvalidConfigException, IOException {
        TestExecutorService executorService = new TestExecutorService("testRemoveStoreNoDequeue");
        
        final BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        final StoreId storeId = new StoreIdImpl("StoreId");
        final String readerId = "reader";
        
        final SeqStoreManagerV3<BasicInflightInfo> manager = createManager(
                configProvider, new AlwaysIncreasingClock(), false);
        
        try {
            // Ensure that the message isn't removed in time there is no reader
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setGuaranteedRetentionPeriod(10000); 
            storeConfig.setMaxMessageLifeTime(-1);
            configProvider.putStoreConfig(storeId.getGroupName(), storeConfig);
            
            manager.createStore(storeId);
            final SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
            final SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
            assertEquals( Collections.singleton( readerId ), store.getReaderNames() );
            
            TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
            store.enqueue(msgIn1, -1, null);
            
            assertNotNull( reader.dequeue() );
            
            manager.deleteStore(storeId);
            
            try {
                store.getReaderNames();
                fail( "Operation on removed store did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            assertNull( manager.getStore( storeId ) );
            assertFalse( manager.containsStore( storeId ) );
            
            manager.waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            
            final SeqStore<AckIdV3, BasicInflightInfo> newStore = manager.createStore(storeId);
            final SeqStoreReader<AckIdV3, BasicInflightInfo> newReader = newStore.createReader(readerId);
            
            try {
                store.getReaderNames();
                fail( "Operation on removed reader did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            assertNotNull( newStore );
            assertNotNull( newReader );
            
            TestEntry msgIn2 = new TestEntry("This is a Test Message - 2", "msg12");
            newStore.enqueue(msgIn2, -1, null);
            
            InflightEntry<AckIdV3, BasicInflightInfo> entry = newReader.dequeue();
            assertEquals(msgIn2.getLogId(), entry.getLogId());
            
            executorService.shutdownWithin(100, TimeUnit.MILLISECONDS);
            executorService.rethrow();
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testRemoveIfEmptyFromAckMessages() throws Exception {
        TestExecutorService executorService = new TestExecutorService("testRemoveStoreNoDequeue");
        
        final BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        final StoreId storeId = new StoreIdImpl("StoreId");
        final String readerId = "reader";
        
        SettableClock clock = new SettableClock();
        
        final SeqStoreManagerV3<BasicInflightInfo> manager = createManager(
                configProvider, clock, false);
        
        try {
            // Ensure that the message isn't removed in time there is no reader
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setGuaranteedRetentionPeriod(0); 
            storeConfig.setMaxMessageLifeTime(-1);
            configProvider.putStoreConfig(storeId.getGroupName(), storeConfig);
            
            manager.createStore(storeId);
            final SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
            final SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
            assertEquals( Collections.singleton( readerId ), store.getReaderNames() );
            
            assertTrue( store.isEmpty() );
            
            TestEntry msgIn1 = new TestEntry("This is a Test Message - 1", "msg1");
            TestEntry msgIn2 = new TestEntry("This is a Test Message - 2", "msg2");
            store.enqueue(msgIn1, -1, null);
            store.enqueue(msgIn2, -1, null);
            
            store.runCleanupNow();
            assertFalse( store.isEmpty() );
            
            InflightEntry<AckIdV3, BasicInflightInfo> dequeue1 = reader.dequeue();
            InflightEntry<AckIdV3, BasicInflightInfo> dequeue2 = reader.dequeue();
            assertNotNull( dequeue1 );
            assertEquals( msgIn1.getLogId(), dequeue1.getLogId() );
            assertNotNull( dequeue2 );
            assertEquals( msgIn2.getLogId(), dequeue2.getLogId() );
            assertNull( reader.dequeue() );
            
            store.runCleanupNow();
            assertFalse( manager.deleteStoreIfEmpty(storeId, 100) );
            
            clock.increaseTime( 100 );
            
            // The messages haven't been acked so the store shouldn't be deletable even if more than 100ms
            //  have passed
            store.runCleanupNow();
            assertFalse( manager.deleteStoreIfEmpty(storeId, 100) );
            
            reader.ack( dequeue1.getAckId() );
            reader.ack( dequeue2.getAckId() );
            
            store.runCleanupNow();
            assertTrue( store.isEmpty() );
            
            assertFalse( manager.deleteStoreIfEmpty(storeId, 100) );
            clock.increaseTime( 100 );
            assertTrue( manager.deleteStoreIfEmpty(storeId, 100) );
            
            try {
                store.getReaderNames();
                fail( "Operation on removed store did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            assertNull( manager.getStore( storeId ) );
            assertFalse( manager.getStoreNames().contains( storeId ) );
            
            manager.waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            
            final SeqStore<AckIdV3, BasicInflightInfo> newStore = manager.createStore(storeId);
            final SeqStoreReader<AckIdV3, BasicInflightInfo> newReader = newStore.createReader(readerId);
            
            try {
                store.getReaderNames();
                fail( "Operation on removed reader did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            assertNotNull( newStore );
            assertNotNull( newReader );
            
            TestEntry msgIn3 = new TestEntry("This is a Test Message - 2", "msg12");
            newStore.enqueue(msgIn3, -1, null);
            
            InflightEntry<AckIdV3, BasicInflightInfo> entry = newReader.dequeue();
            assertEquals(msgIn3.getLogId(), entry.getLogId());
            
            executorService.shutdownWithin(100, TimeUnit.MILLISECONDS);
            executorService.rethrow();
        } finally {
            manager.close();
        }
    }
    
    @Test
    public void testRemoveIfEmptyFromExpiration() throws Exception {
        TestExecutorService executorService = new TestExecutorService("testRemoveStoreNoDequeue");
        
        final BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        final StoreId storeId = new StoreIdImpl("StoreId");
        final String readerId = "reader";
        
        SettableClock clock = new SettableClock();
        
        final SeqStoreManagerV3<BasicInflightInfo> manager = createManager(
                configProvider, clock, false);
        
        try {
            // Ensure that the message isn't removed in time there is no reader
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            storeConfig.setGuaranteedRetentionPeriod(0); 
            storeConfig.setMaxMessageLifeTime(1000);
            configProvider.putStoreConfig(storeId.getGroupName(), storeConfig);
            
            manager.createStore(storeId);
            final SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
            final SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
            assertEquals( Collections.singleton( readerId ), store.getReaderNames() );
            
            assertTrue( store.isEmpty() );
            
            TestEntry msgIn1 = new TestEntry("This is a Test Message - 1", "msg1");
            TestEntry msgIn2 = new TestEntry("This is a Test Message - 2", "msg2");
            store.enqueue(msgIn1, -1, null);
            store.enqueue(msgIn2, -1, null);
            
            store.runCleanupNow();
            assertFalse( store.isEmpty() );
            
            InflightEntry<AckIdV3, BasicInflightInfo> dequeue1 = reader.dequeue();
            InflightEntry<AckIdV3, BasicInflightInfo> dequeue2 = reader.dequeue();
            assertNotNull( dequeue1 );
            assertEquals( msgIn1.getLogId(), dequeue1.getLogId() );
            assertNotNull( dequeue2 );
            assertEquals( msgIn2.getLogId(), dequeue2.getLogId() );
            assertNull( reader.dequeue() );
            
            store.runCleanupNow();
            assertFalse( manager.deleteStoreIfEmpty(storeId, 100) );
            
            clock.increaseTime( 100 );
            
            store.runCleanupNow();
            assertFalse( manager.deleteStoreIfEmpty(storeId, 100) );
            
            // Increase time passed the maximum retention period
            clock.increaseTime( 1000 );
            
            store.runCleanupNow();
            assertTrue( store.isEmpty() );
            
            assertFalse( manager.deleteStoreIfEmpty(storeId, 100) );
            
            clock.increaseTime( 100 );
            assertTrue( manager.deleteStoreIfEmpty(storeId, 100) );
            
            try {
                store.getReaderNames();
                fail( "Operation on removed store did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            assertNull( manager.getStore( storeId ) );
            assertFalse( manager.containsStore( storeId ) );
            
            manager.waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            
            final SeqStore<AckIdV3, BasicInflightInfo> newStore = manager.createStore(storeId);
            final SeqStoreReader<AckIdV3, BasicInflightInfo> newReader = newStore.createReader(readerId);
            
            try {
                store.getReaderNames();
                fail( "Operation on removed reader did not fail.");
            } catch( SeqStoreClosedException e ) {
                // Success
            }
            
            assertNotNull( newStore );
            assertNotNull( newReader );
            
            TestEntry msgIn3 = new TestEntry("This is a Test Message - 2", "msg12");
            newStore.enqueue(msgIn3, -1, null);
            
            InflightEntry<AckIdV3, BasicInflightInfo> entry = newReader.dequeue();
            assertEquals(msgIn3.getLogId(), entry.getLogId());
            
            executorService.shutdownWithin(100, TimeUnit.MILLISECONDS);
            executorService.rethrow();
        } finally {
            manager.close();
        }
    }
}
