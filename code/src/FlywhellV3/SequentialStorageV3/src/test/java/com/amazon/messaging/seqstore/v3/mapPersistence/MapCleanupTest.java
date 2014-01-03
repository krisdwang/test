package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.io.IOException;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.CleanupTestBase;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;


public class MapCleanupTest extends CleanupTestBase {
    
    public MapCleanupTest() {
        super(false);
    }
    
    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(
                                                                     Clock clock,
                                                                     BasicConfigProvider<BasicInflightInfo> configProvider,
                                                                     boolean truncate)
            throws InvalidConfigException, SeqStoreException, IOException 
    {
        return new MapStoreManager<BasicInflightInfo>( configProvider, clock );
    }
    
    @Override
    protected boolean isManagerStatePersistent() {
        return false;
    }
}
