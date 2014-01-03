package com.amazon.messaging.seqstore.v3.bdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreReaderV3TestBase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

public class BDBSeqStoreReaderV3Test extends SeqStoreReaderV3TestBase {
    private BDBStoreManager<BasicInflightInfo> manager;

    @Override
    public void setUp(Clock clock, SeqStoreReaderConfig<BasicInflightInfo> config) throws Exception {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        BasicConfigProvider<BasicInflightInfo> confProvider =
        	BasicConfigProvider.newBasicInflightConfigProvider();
        confProvider.putReaderConfig(STOREID.getStoreName(), "reader", config);
        manager = BDBStoreManager.createDefaultTestingStoreManager(
                confProvider, con.getImmutableConfig(), clock);
        manager.createStore(STOREID);
        store = manager.getStore(STOREID);
        store.createReader("reader");
        source = store.getReader("reader");
    }

    @After
    @Override
    public void tearDown() throws Exception {
        manager.close();
    }

    @Test
    public void restoreTest() throws Exception {
        setUp(new AlwaysIncreasingClock());
        loadStore(1000);
        int numToDequeue = 500;
        for (int i = 0; i < numToDequeue; i++) {
            AckIdV3 id = source.dequeue().getAckId();
            source.ack(id);
        }

        manager.close();
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
        
        manager = BDBStoreManager.createDefaultTestingStoreManager(
                BasicConfigProvider.newBasicInflightConfigProvider(), con.getImmutableConfig(), 
                new AlwaysIncreasingClock());
        
        store = manager.getStore(STOREID);
        assertNotNull(store);
        source = store.getReader("reader");
        String result = source.dequeue().getLogId();
        assertEquals(numToDequeue, Integer.parseInt(result));
    }
    
}
