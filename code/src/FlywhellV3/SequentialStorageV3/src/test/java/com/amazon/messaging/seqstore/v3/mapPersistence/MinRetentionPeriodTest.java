package com.amazon.messaging.seqstore.v3.mapPersistence;

import org.junit.Before;

import com.amazon.messaging.seqstore.v3.MinRetentionPeriodTestBase;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.util.BasicInflightInfo;

public class MinRetentionPeriodTest extends MinRetentionPeriodTestBase {

    private MapStoreManager<BasicInflightInfo> manager_;

    @Before
    @Override
    public void setUp() throws SeqStoreException {
        manager_ = new MapStoreManager<BasicInflightInfo>(confProvider, clock_);

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
