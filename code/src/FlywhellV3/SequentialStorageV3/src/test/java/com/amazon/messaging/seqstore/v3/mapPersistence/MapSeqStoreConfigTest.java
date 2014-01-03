package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.io.IOException;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.SeqStoreConfigTestBase;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Scheduler;


public class MapSeqStoreConfigTest extends SeqStoreConfigTestBase {
    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(
        Clock clock, ConfigProvider<BasicInflightInfo> configProvider, Scheduler scheduler)
        throws InvalidConfigException, SeqStoreException, IOException
    {
        return new MapStoreManager<BasicInflightInfo>(
                configProvider,  new NonPersistentBucketCreator<BasicInflightInfo>(),
                scheduler, clock);
    }
}
