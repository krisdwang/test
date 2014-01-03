package com.amazon.messaging.seqstore.v3.store;

import java.util.Random;
import java.util.concurrent.CyclicBarrier;

import org.junit.After;
import org.junit.Before;

import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.BucketStorageManager;
import com.amazon.messaging.seqstore.v3.internal.DefaultAckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.mapPersistence.NonPersistentBucketCreator;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Scheduler;

public class StoreImplTest extends StoreImplTestBase {


    @Override
    @Before
    public void setup() throws Exception {
        ackIdGen = new AckIdGenerator();
        clock_ = new SettableClock();
        
        StorePersistenceManager storePersistenceManager =
                new NonPersistentBucketCreator<BasicInflightInfo>();
        
        BucketStorageManager bucketStorageManager = 
                new BucketStorageManager(storePersistenceManager, Scheduler.getGlobalInstance(), new NullMetricsFactory() );

        store_ = new Store(
                new StoreIdImpl("test"), 
                bucketStorageManager,
                storeConfig,
                new DefaultAckIdSourceFactory(ackIdGen, clock_ ),
                new StoreSpecificClock( clock_ ) );

        random_ = new Random(10);
        numMsg_ = random_.nextInt(MAX_MSG);
        numReaders_ = 10;
        numEnqueuers_ = 10;
        barrier_ = new CyclicBarrier(numReaders_ + numEnqueuers_ + 1);
    }

    @Override
    @After
    public void shutdown() throws Exception {
        store_.close();
    }

}
