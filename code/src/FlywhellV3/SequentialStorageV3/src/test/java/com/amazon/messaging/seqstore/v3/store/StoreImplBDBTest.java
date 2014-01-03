package com.amazon.messaging.seqstore.v3.store;

import java.util.Random;
import java.util.Vector;
import java.util.concurrent.CyclicBarrier;

import org.junit.After;
import org.junit.Before;

import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.bdb.BDBPersistentManager;
import com.amazon.messaging.seqstore.v3.bdb.BDBStoreManager;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.BucketStorageManager;
import com.amazon.messaging.seqstore.v3.internal.DefaultAckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Scheduler;

public class StoreImplBDBTest extends StoreImplTestBase {

    protected BDBPersistentManager bcreater_;
    
    protected BucketStorageManager bucketStorageManager_;

    @Override
    @Before
    public void setup() throws Exception {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        ackIdGen = new AckIdGenerator();
        clock_ = new SettableClock();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        
        BDBStoreManager<BasicInflightInfo> manager_ = BDBStoreManager.createDefaultTestingStoreManager(
        		BasicConfigProvider.newBasicInflightConfigProvider(), con.getImmutableConfig(), ackIdGen, clock_);

        bcreater_ = (BDBPersistentManager) (manager_.getPersistenceManager());
        
        bucketStorageManager_ = new BucketStorageManager(bcreater_, Scheduler.getGlobalInstance(), new NullMetricsFactory() );
        
        store_ = 
            new Store(
                    new StoreIdImpl("test"), 
                    bucketStorageManager_,
                    storeConfig, 
                    new DefaultAckIdSourceFactory( ackIdGen, clock_), 
                    new StoreSpecificClock( clock_ ) );

        random_ = new Random(10);
        numMsg_ = random_.nextInt(MAX_MSG);
        ids_ = new Vector<AckIdV3>();
        payloads_ = new Vector<byte[]>();
        numReaders_ = 10;
        numEnqueuers_ = 10;
        barrier_ = new CyclicBarrier(numReaders_ + numEnqueuers_ + 1);

    }

    @Override
    @After
    public void shutdown() throws Exception {
        store_.close();
        bucketStorageManager_.deleteBucketsForStore(store_.getDestinationId(), null);
        bucketStorageManager_.shutdown();
        bcreater_.close();
    }

}
