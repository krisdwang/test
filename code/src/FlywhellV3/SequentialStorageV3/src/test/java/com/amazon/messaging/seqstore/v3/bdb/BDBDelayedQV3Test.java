package com.amazon.messaging.seqstore.v3.bdb;

import java.io.IOException;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.DelayedQV3TestBase;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;

public class BDBDelayedQV3Test extends DelayedQV3TestBase {

    @Override
    public void setUp() throws SeqStoreException, IOException {

        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        manager_ = BDBStoreManager.createDefaultTestingStoreManager(
                configProvider, con.getImmutableConfig(), new AlwaysIncreasingClock());
    }

    @Override
    public void tearDown() throws SeqStoreException {
        manager_.close();
    }

}
