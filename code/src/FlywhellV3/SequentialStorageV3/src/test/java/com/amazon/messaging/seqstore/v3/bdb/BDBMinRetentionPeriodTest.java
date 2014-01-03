package com.amazon.messaging.seqstore.v3.bdb;

import java.io.IOException;

import org.junit.Before;

import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.MinRetentionPeriodTestBase;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.util.BasicInflightInfo;

public class BDBMinRetentionPeriodTest extends MinRetentionPeriodTestBase {

    BDBStoreManager<BasicInflightInfo> manager_;
    @Before
    @Override
    public void setUp() throws SeqStoreException, IOException {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        manager_ = BDBStoreManager.createDefaultTestingStoreManager(
                confProvider, con.getImmutableConfig(),clock_);
    }

    @Override
    public void tearDown() throws SeqStoreException {
        manager_.close();
    }

    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> getManager() {
        return manager_;
    }

}
