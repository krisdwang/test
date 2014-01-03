package com.amazon.messaging.seqstore.v3.bdb;

import java.io.IOException;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.SeqStorePersistencePerfTestBase;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.util.BasicInflightInfo;

public class BDBSeqStorePersistencePerfTest extends SeqStorePersistencePerfTestBase {

    public BDBSeqStorePersistencePerfTest() {
        super(2048);
    }

    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> setupManager(Clock clock)
            throws SeqStoreException, IOException {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        BDBStoreManager<BasicInflightInfo> manager = 
            BDBStoreManager.createDefaultTestingStoreManager(
                confProvider, con.getImmutableConfig(), clock);
        manager.createStore(STOREID1);
        manager.createStore(STOREID2);
        return manager;
    }
    
    @Override
    public void shutdownManager(SeqStoreManagerV3<BasicInflightInfo> manager) throws SeqStoreException {
        manager.printDBStats();
        manager.close();
    }
}
