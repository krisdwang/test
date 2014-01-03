package com.amazon.messaging.seqstore.v3.mapPersistence;

import org.junit.After;
import org.junit.Before;

import com.amazon.messaging.seqstore.v3.MaxRetentionPeriodTestBase;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.util.BasicInflightInfo;

public class MaxRetentionPeriodTest extends MaxRetentionPeriodTestBase {

    @Override
    @Before
    public void setUp() throws SeqStoreException {
        manager_ = new MapStoreManager<BasicInflightInfo>(confProvider, clock);

        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setMaxMessageLifeTime(INTERVAL);
        confProvider.putStoreConfig("foo", storeConfig);
        store_ = manager_.createStore(new StoreIdImpl("foo"));
        // manager_.setEnforcementIntervalMS(3 * 1000); // 3 seconds
        store_.createReader("reader-1");
    }

    @Override
    @After
    public void tearDown() throws SeqStoreException {
        manager_.close();
    }

}
