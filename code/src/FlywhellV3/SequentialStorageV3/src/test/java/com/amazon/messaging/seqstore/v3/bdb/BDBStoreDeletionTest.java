package com.amazon.messaging.seqstore.v3.bdb;

import java.io.IOException;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreDeletionTestBase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;


public class BDBStoreDeletionTest extends StoreDeletionTestBase {
    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> createManager( 
            BasicConfigProvider<BasicInflightInfo> configProvider, Clock clock, boolean truncate) 
            throws IOException, InvalidConfigException, SeqStoreException
    {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        return BDBStoreManager.createDefaultTestingStoreManager(
                configProvider, con.getImmutableConfig(), clock);
    }
}
