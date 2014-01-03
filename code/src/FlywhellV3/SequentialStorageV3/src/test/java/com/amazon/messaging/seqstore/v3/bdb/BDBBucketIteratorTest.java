package com.amazon.messaging.seqstore.v3.bdb;

import java.io.IOException;
import java.util.List;

import org.junit.runner.RunWith;

import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.BucketIteratorTestBase;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.testing.LabelledParameterized;
import com.amazon.messaging.testing.LabelledParameterized.Parameters;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;
import com.amazon.messaging.util.BDBBucketTestParameters;
import com.amazon.messaging.utils.Scheduler;

@RunWith(LabelledParameterized.class)
public class BDBBucketIteratorTest  extends BucketIteratorTestBase {
    @Parameters
    public static List<TestParameters> getTestParameters() {
        return BDBBucketTestParameters.getStorageTypeTestParameters();
    }
    
    public BDBBucketIteratorTest(BucketStorageType storageType) {
        super( storageType );
    }

    @Override
    protected StorePersistenceManager createPersistenceManager()
            throws SeqStoreException, InvalidConfigException, IOException
    {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        con.setTruncateDatabase(true);
        return new BDBPersistentManager( con.getImmutableConfig(), Scheduler.getGlobalInstance() );
    }

}
