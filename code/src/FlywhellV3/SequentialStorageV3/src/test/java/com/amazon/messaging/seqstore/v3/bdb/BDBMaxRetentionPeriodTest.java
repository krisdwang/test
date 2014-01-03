package com.amazon.messaging.seqstore.v3.bdb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;

import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.MaxRetentionPeriodTestBase;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

public class BDBMaxRetentionPeriodTest extends MaxRetentionPeriodTestBase {

    @Override
    @Before
    public void setUp() throws SeqStoreException, IOException {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        manager_ = BDBStoreManager.createDefaultTestingStoreManager(
                confProvider, con.getImmutableConfig(),clock);
        
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setMaxMessageLifeTime(INTERVAL);
        StoreId id = new StoreIdImpl("foo", "1");
        confProvider.putStoreConfig(id.getGroupName(), storeConfig);

        manager_.createStore(id);
        // manager_.setEnforcementIntervalMS(3 * 1000); // 3 seconds

        store_ = manager_.getStore(id);
        store_.createReader("reader-1");
    }

    @Override
    @After
    public void tearDown() throws SeqStoreException {
        manager_.close();
    }

}
