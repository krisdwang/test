package com.amazon.messaging.seqstore.v3.mapPersistence;

import org.junit.After;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreReaderV3TestBase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

public class SeqStoreReaderV3Test extends SeqStoreReaderV3TestBase {

    private MapStoreManager<BasicInflightInfo> manager;

    @Override
    public void setUp(Clock clock, SeqStoreReaderConfig<BasicInflightInfo> config) throws Exception {
    	BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
    	configProvider.putReaderConfig(STOREID.getStoreName(), "reader", config);
        manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock);
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
}
