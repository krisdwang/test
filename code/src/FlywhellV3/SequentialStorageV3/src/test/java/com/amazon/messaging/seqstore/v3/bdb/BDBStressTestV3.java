package com.amazon.messaging.seqstore.v3.bdb;

import java.io.IOException;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.StressTestV3Base;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

public class BDBStressTestV3 extends StressTestV3Base {

    @Override
    protected void setUp() throws IOException, InvalidConfigException, SeqStoreException {
        basicSetup();
        stores_.clear();
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        try {
            manager_ = BDBStoreManager.createDefaultTestingStoreManager(
                    BasicConfigProvider.newBasicInflightConfigProvider(), con.getImmutableConfig(),
                    new AlwaysIncreasingClock() );
        } catch (SeqStoreInternalException e) {
            throw new IllegalStateException("fail to start", e);
        }

    }

    @Override
    protected void shutdown() throws SeqStoreException {
        if( manager_ != null ) manager_.close();
    }

}
