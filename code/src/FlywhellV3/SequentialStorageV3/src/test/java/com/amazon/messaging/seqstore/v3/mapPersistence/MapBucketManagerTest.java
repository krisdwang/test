package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.io.IOException;

import org.junit.After;

import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.config.BucketStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.BucketStorageManager;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.BucketManager;
import com.amazon.messaging.seqstore.v3.store.BucketManagerTestBase;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Scheduler;

public class MapBucketManagerTest extends BucketManagerTestBase {
    
    private NonPersistentBucketCreator<BasicInflightInfo> manager;
    
    public MapBucketManagerTest() {
        // the map bucket manager doesn't support shared database buckets
        super(false);
    }
    
    @After
    public void cleanup() {
        // Make sure managers don't get reused
        if( manager != null ) {
            manager.close();
            manager = null;
        }
    }

    @Override
    public BucketManager createBucketManager(Clock clock,
            boolean clearExistingStore) throws IOException, SeqStoreException {
        manager =
        	new NonPersistentBucketCreator< BasicInflightInfo>();
        
        return new BucketManager(
                new StoreIdImpl("test"), STORE_CONFIG, 
                new BucketStorageManager(manager, Scheduler.getGlobalInstance(), new NullMetricsFactory() ) );
    }
    
    @Override
    protected void shutdownBucketManager(BucketManager bm) throws SeqStoreException {
        bm.close();
        manager.close();
        manager = null;
    }
    

    @Override
    protected StorePersistenceManager getPersistenceManager() {
        return manager;
    }
    
    @Override
    protected BucketStoreConfig getDefaultConfig() {
        return getConfig(BucketStorageType.DedicatedDatabase);
    }
}
