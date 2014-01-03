package com.amazon.messaging.seqstore.v3.bdb;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.BasicSeqStoreTestBase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Scheduler;

public class BDBBasicSeqStoreTest extends BasicSeqStoreTestBase {

    SeqStorePersistenceConfig con;
    
    @Override
    public void setUp() throws SeqStoreException, IOException {
        con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        scheduler = new Scheduler("BDBBasicSeqStoreTest scheduler thread", 5);
        scheduler.setRequireCleanShutdown(true);
        manager = BDBStoreManager.createDefaultTestingStoreManager(
                configProvider, con.getImmutableConfig(), clock_, scheduler );
    }

    @Override
    public void tearDown() throws SeqStoreException {
        manager.close();
        assertTrue( scheduler.isShutdown() );
    }
    
    @Test
    public void testRestore() throws SeqStoreException, InterruptedException {
        manager.createStore(storeId);
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
        TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
        store.enqueue(msgIn1, -1, null);
        StoredEntry<AckIdV3> msgOut1 = reader.dequeue();
        assertEquals(msgIn1.getLogId(), msgOut1.getLogId());
        assertEquals(new String(msgIn1.getPayload()), new String(msgOut1.getPayload()));
        manager.close();
        manager = BDBStoreManager.createDefaultTestingStoreManager(configProvider, con.getImmutableConfig(), clock_);
        store = manager.getStore(storeId);
        reader = store.createReader(readerId);
        StoredEntry<AckIdV3> msgOut2 = reader.dequeue();
        assertEquals(msgIn1.getLogId(), msgOut2.getLogId());
        assertEquals(new String(msgIn1.getPayload()), new String(msgOut2.getPayload()));        
        reader.ack(msgOut2.getAckId());
        manager.deleteStore(storeId);
    }

    @Test
    public void testRestoreWithDelayed() throws SeqStoreException, InterruptedException {
        manager.createStore(storeId);
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
        TestEntry msgIn1 = new TestEntry("This is a Test Message".getBytes(), 1000, "msg1");
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
        store.enqueue(msgIn1, -1, null);
        StoredEntry<AckIdV3> msgOut1 = reader.dequeue();
        assertNull(msgOut1);
        manager.close();
        manager = BDBStoreManager.createDefaultTestingStoreManager(configProvider, con.getImmutableConfig(), clock_);
        store = manager.getStore(storeId);
        reader = store.createReader(readerId);
        msgOut1 = reader.dequeue();
        assertNull(msgOut1);
        clock_.setCurrentTime(1000);
        StoredEntry<AckIdV3> msgOut2 = reader.dequeue();
        assertEquals(msgIn1.getLogId(), msgOut2.getLogId());
        assertEquals(new String(msgIn1.getPayload()), new String(msgOut2.getPayload()));        
        reader.ack(msgOut2.getAckId());
        manager.deleteStore(storeId);
    }
    
    @Test
    public void testCloseStore() throws SeqStoreException, InterruptedException {
        manager.createStore(storeId);
        SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(storeId);
        SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.createReader(readerId);
        
        assertTrue( store.isOpen() );
        
        TestEntry msgIn1 = new TestEntry("This is a Test Message", "msg1");
        store.enqueue(msgIn1, -1, null);

        assertEquals( Collections.singleton( storeId ), manager.getStoreNames() );
        manager.closeStore(storeId);
        assertEquals( Collections.singleton( storeId ), manager.getStoreNames() );
        assertFalse( store.isOpen() );

        try {
            store.getReaderNames();
            fail( "Operation on closed store did not fail.");
        } catch( SeqStoreClosedException e ) {
            // Success
        }
        
        try {
            reader.getInflightMetrics();
            fail( "Operation on closed reader did not fail.");
        } catch( SeqStoreClosedException e ) {
            // Success
        }
        
        store = manager.getStore(storeId);
        assertNotNull(store);
        assertEquals( Collections.singleton( storeId ), manager.getStoreNames() );
        assertTrue( store.isOpen() );
        
        try {
            reader.getInflightMetrics();
            fail( "Operation on closed reader did not fail after store was reloaded.");
        } catch( SeqStoreClosedException e ) {
            // Success
        }
        
        reader = store.getReader(readerId);
        assertNotNull(reader);
        InflightEntry<AckIdV3, BasicInflightInfo> entry = reader.dequeue();
        assertNotNull(entry);
        assertEquals(msgIn1.getLogId(), entry.getLogId());
    }
}
