package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.io.IOException;

import com.amazon.messaging.seqstore.v3.BucketIteratorTestBase;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.util.BasicInflightInfo;


public class MapBucketIteratorTest extends BucketIteratorTestBase {
    public MapBucketIteratorTest() {
        super( BucketStorageType.DedicatedDatabase );
    }
    
    @Override
    protected StorePersistenceManager createPersistenceManager()
            throws SeqStoreException, InvalidConfigException, IOException
    {
        return new NonPersistentBucketCreator<BasicInflightInfo>();
    }

}
