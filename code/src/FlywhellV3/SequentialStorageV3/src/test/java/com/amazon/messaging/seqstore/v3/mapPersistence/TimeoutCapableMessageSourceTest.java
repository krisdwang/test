package com.amazon.messaging.seqstore.v3.mapPersistence;

import org.junit.After;
import org.junit.Before;

import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internal.TimeoutCapableMessageSourceTestBase;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;

public class TimeoutCapableMessageSourceTest extends TimeoutCapableMessageSourceTestBase {

    MapStoreManager<BasicInflightInfo> manager = null;

    @Override
    @Before
    public void setUp() throws Exception {
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        manager = new MapStoreManager<BasicInflightInfo>(configProvider, clock_);
        SeqStoreInternalInterface<BasicInflightInfo> store = manager.createStore(new StoreIdImpl("store"));

        for (int i = 0; i < 15; i++) {
            TestEntry entry = new TestEntry("message - " + i);
            store.enqueue(entry, -1, null);
        }

        BasicInflightInfoFactory infoFactory =
        	new BasicInflightInfoFactory(INTERVAL);
        SeqStoreReaderConfig<BasicInflightInfo> config =
        	new SeqStoreReaderConfig<BasicInflightInfo>(infoFactory);
        configProvider.putReaderConfig(store.getConfigKey(), "reader", config);
        store.createReader("reader");
        source = (SeqStoreReaderV3InternalInterface<BasicInflightInfo>) store.getReader("reader");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        manager.close();
    }

}
