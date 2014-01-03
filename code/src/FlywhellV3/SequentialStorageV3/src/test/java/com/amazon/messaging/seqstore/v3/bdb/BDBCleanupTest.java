package com.amazon.messaging.seqstore.v3.bdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.runner.RunWith;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.CleanupTestBase;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.testing.LabelledParameterized;
import com.amazon.messaging.testing.LabelledParameterized.Parameters;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

@RunWith(LabelledParameterized.class)
public class BDBCleanupTest extends CleanupTestBase {
    @Parameters
    public static List<TestParameters> getParams() {
        return Arrays.asList( 
                new TestParameters("!UseLLMEnv", new Object[] { false } ),
                new TestParameters("UseLLMEnv", new Object[] { true } ) );
    }
    
    @Override
    protected SeqStoreManagerV3<BasicInflightInfo> getManager(
            Clock clock, BasicConfigProvider<BasicInflightInfo> configProvider, boolean truncate) 
            throws InvalidConfigException, SeqStoreException, IOException 
    {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        if( truncate ) {
            con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        } else {
            con.setStoreDirectory(SeqStoreTestUtils.getBDBDir(getClass().getSimpleName()));
        }
        con.setUseSeperateEnvironmentForDedicatedBuckets(useLLMEnv);
        return BDBStoreManager.createDefaultTestingStoreManager(
                configProvider, con.getImmutableConfig(),clock);
    }
    
    public BDBCleanupTest(boolean allowShared) {
        super( allowShared );
    }
    
    /**
     * Should return true if the manager state is persistent across close and recreate.
     * 
     * @return
     */
    @Override
    protected boolean isManagerStatePersistent() {
        return true;
    }
}
