package com.amazon.messaging.seqstore.v3.mapPersistence;

import org.junit.After;
import org.junit.Before;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.seqstore.v3.DelayedQV3TestBase;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.util.BasicInflightInfo;

/**
 * Copied mostly from com.amazon.messaging.seqstore.impl.DelayedQTest.java
 * Modified to use the v3 classes.
 */
public class DelayedQV3Test extends DelayedQV3TestBase {

    @Before
    @Override
    public void setUp() throws SeqStoreException {
        /*
         * BDBSeqStoreManagerPersistenceConfig conf = new
         * BDBSeqStoreManagerPersistenceConfig(); File storeDir = new
         * File("DelayedQsTest-testdata"); if (!storeDir.exists()) {
         * storeDir.mkdirs(); // insure that the store directory exists }
         * conf.setStoreDirectory(storeDir); conf.setTruncateDatabase(true); //
         * *DANGERIOUS* wipes out
         */

        manager_ = new MapStoreManager<BasicInflightInfo>(configProvider, new AlwaysIncreasingClock());
        /*
         * SeqStoreManagerPersistenceConfig con = new
         * SeqStoreManagerPersistenceConfig(); File storeFolder = new
         * File("testdata"); storeFolder.mkdirs();
         * con.setStoreDirectory(storeFolder); manager_ = new
         * BDBStoreManager(con);
         */
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setGuaranteedRetentionPeriodSeconds(30);

        store_ = manager_.getStore(DESTINATION);
    }

    @After
    @Override
    public void tearDown() throws SeqStoreException {
        msgCounter_.set(0);
        threadCounter_.set(0);
        manager_.close();
    }

}
