//brazil/src/shared/platform/SequentialStorage/mainline/src/test/java/com/amazon/messaging/seqstore/persistence/SeqStorePersistencePerfTestBase.java#3 - edit change 1276243 (ktext)
package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.io.IOException;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.SeqStorePersistencePerfTestBase;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.util.BasicInflightInfo;

/**
 * Tests performance of persistence layer. To test concrete implementaion create
 * subclass that initializes manager field in {@link #setUp(Clock)} method.
 * 
 * @author fateev
 */
public class SeqStorePersistencePerfTest extends SeqStorePersistencePerfTestBase {

    public SeqStorePersistencePerfTest() {
        super(2);
    }

    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> setupManager(Clock clock)
            throws SeqStoreException, IOException {
        MapStoreManager<BasicInflightInfo> manager = new MapStoreManager<BasicInflightInfo>(confProvider, clock);
        manager.createStore(STOREID1);
        manager.createStore(STOREID2);
        return manager;
    }

    @Override
    public void shutdownManager(SeqStoreManagerV3<BasicInflightInfo> manager) throws SeqStoreException {
        manager.close();
    }
}
