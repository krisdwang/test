package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.io.IOException;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.CheckpointTestBase;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.DefaultAckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.util.BasicInflightInfo;


public class CheckpointTest extends CheckpointTestBase {

    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(Clock clock,
                                                                     ConfigProvider<BasicInflightInfo> configProvider,
                                                                     CheckpointProvider<StoreId, AckIdV3, AckIdV3, BasicInflightInfo> checkpointProvider)
            throws InvalidConfigException, SeqStoreException, IOException 
    {
        return new MapStoreManager<BasicInflightInfo>(
                configProvider, checkpointProvider, 
                new DefaultAckIdSourceFactory( new AckIdGenerator( clock.getCurrentTime() ), clock), 
                new StoreSpecificClock(clock) );
    }

}
