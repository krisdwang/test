package com.amazon.messaging.seqstore.v3.bdb;

import org.junit.After;
import org.junit.Before;

import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internal.TimeoutCapableMessageSourceTestBase;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;

public class BDBTimeoutCapableMessageSourceTest extends TimeoutCapableMessageSourceTestBase {

    private SeqStoreInternalInterface<BasicInflightInfo> store;

    private BasicConfigProvider<BasicInflightInfo> configProvider =
    	BasicConfigProvider.newBasicInflightConfigProvider();

    private BDBStoreManager<BasicInflightInfo> manager_;

    @Before
    @Override
    public void setUp() throws Exception {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        manager_ = BDBStoreManager.createDefaultTestingStoreManager(configProvider, con.getImmutableConfig(), clock_);
        store = manager_.createStore(new StoreIdImpl("store"));
        for (int i = 0; i < 15; i++) {
            TestEntry entry = new TestEntry("message - " + i);
            store.enqueue(entry, -1, null);
        }

        BasicInflightInfoFactory infoFactory = new BasicInflightInfoFactory(INTERVAL);
        SeqStoreReaderConfig<BasicInflightInfo> config = new SeqStoreReaderConfig<BasicInflightInfo>(infoFactory);
        configProvider.putReaderConfig(store.getConfigKey(), "reader", config);
        store.createReader("reader");
        source = store.getReader("reader");
    }

    @After
    @Override
    public void tearDown() throws Exception {
       manager_.close();
    }

}
