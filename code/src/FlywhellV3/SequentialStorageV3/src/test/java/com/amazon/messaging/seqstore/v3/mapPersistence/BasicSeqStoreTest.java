package com.amazon.messaging.seqstore.v3.mapPersistence;

import static org.junit.Assert.assertTrue;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.BasicSeqStoreTestBase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Scheduler;

/**
 * Based on
 * com.amazon.messaging.seqstore.v2.persistence.bdb.BDBSeqStoreTest.java
 * 
 * @author robburke
 */
public class BasicSeqStoreTest extends BasicSeqStoreTestBase {

    @Override 
    public void setUp() throws SeqStoreException {
        configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        scheduler = new Scheduler("BDBBasicSeqStoreTest scheduler thread", 5);
        scheduler.setRequireCleanShutdown(true);
        manager = new MapStoreManager<BasicInflightInfo>(configProvider, scheduler, clock_ );
    }

    @Override
    public void tearDown() throws SeqStoreException {
        manager.close();
        assertTrue( scheduler.isShutdown() );
    }
}
