package com.amazon.messaging.seqstore.v3.bdb;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.BucketCursorTestBase;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.testing.LabelledParameterized;
import com.amazon.messaging.testing.LabelledParameterized.Parameters;
import com.amazon.messaging.testing.LabelledParameterized.TestParameters;
import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

@RunWith(LabelledParameterized.class)
public class BDBBucketCursorTest extends BucketCursorTestBase {
    private Environment env;
    
    private final Set<BDBBackedBucketStore> openBucketStores = 
            Collections.synchronizedSet( new HashSet<BDBBackedBucketStore>() );
    
    private final AtomicInteger bucketIdCounter = new AtomicInteger(0);
    
    private BucketStorageType bucketStorageType;
    
    private Database sharedDB;
    
    @Parameters
    public static List<TestParameters> getTestParameters() {
        return Arrays.asList(  
                new TestParameters( "Dedicated", new Object[] { BucketStorageType.DedicatedDatabase } ),
                new TestParameters( "Shared", new Object[] { BucketStorageType.SharedDatabase } ) );
    }
    
    public BDBBucketCursorTest(BucketStorageType bucketStorageType) {
        this.bucketStorageType = bucketStorageType;
    }
    
    @Before
    public void setup() throws EnvironmentLockedException, DatabaseException, IOException {
        File bdbDir = SeqStoreTestUtils.setupBDBDir(BDBBucketCursorTest.class.getSimpleName());
        
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(false);
        envConfig.setAllowCreate(true); // if environment does not exist when

        envConfig.setCacheSize(20 * 1024 * 1024);
        // envConfig.setCachePercent(30);
        envConfig.setTransactional(true);
        envConfig.setLockTimeout(5,TimeUnit.SECONDS );
        envConfig.setConfigParam(EnvironmentConfig.ENV_DB_EVICTION, "true");
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION, "20");
        envConfig.setConfigParam(EnvironmentConfig.TXN_DUMP_LOCKS, "true");

        env = new Environment(bdbDir, envConfig);
        if( bucketStorageType == BucketStorageType.SharedDatabase ) {
            sharedDB = DatabaseUtil.createDB(env, "SHARED_BUCKET_DB", true, false, false);
        }
    }
    
    @After
    public void shutdown() throws DatabaseException, SeqStoreDatabaseException {
        if( env != null ) {
            for( BDBBackedBucketStore store : openBucketStores ) {
                store.close();
            }
            
            if( bucketStorageType == BucketStorageType.SharedDatabase ) {
                sharedDB.close();
            }
            
            env.close();
        }
    }
    
    @Override
    public BucketStore getBucketStore(String name) throws SeqStoreException {
        StoreId storeId = new StoreIdImpl( name, null);
        AckIdV3 bucketId = new AckIdV3( 5, true );
        String bucketName = BDBPersistentManager.getBucketName(storeId, bucketId);
        
        TransactionConfig tconfig = new TransactionConfig();
        tconfig.setReadCommitted(true);
        
        Transaction txn;
        try {
            txn = CurrentTransaction.getInstance(env).beginTransaction(tconfig);
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        }
        try {
            txn.setName(Thread.currentThread().getName() + ":createBucket");
            
            BDBBackedBucketStore retval;
            
            if( bucketStorageType == BucketStorageType.DedicatedDatabase ) {
                long bucketSequenceId;
                bucketSequenceId = bucketIdCounter.getAndIncrement();
                
                retval = BDBBucketStore.getOrCreateBucketStore( 
                        env, txn, BucketStorageType.DedicatedDatabase, bucketId, bucketSequenceId, bucketName, false, true);
            } else if( bucketStorageType == BucketStorageType.SharedDatabase ) {
                long firstIdToUse = bucketIdCounter.getAndAdd(3);
                
                AckIdV3 prevBucketId = new AckIdV3( 4, true );
                String prevBucketName = BDBPersistentManager.getBucketName(storeId, prevBucketId);
                
                AckIdV3 afterBucketId = new AckIdV3( 6, true );
                String afterBucketName = BDBPersistentManager.getBucketName(storeId, afterBucketId);
                
                txn.setName(Thread.currentThread().getName() + ":createBucket");
                // Create buckets before and after the bucket to use to detect overflow into other buckets
                BDBSharedDBBucketStore prevStore = BDBSharedDBBucketStore.createNewBucketStore(
                        txn, sharedDB, prevBucketId, firstIdToUse, prevBucketName );
                
                BDBSharedDBBucketStore nextStore = BDBSharedDBBucketStore.createNewBucketStore(
                        txn, sharedDB, afterBucketId, firstIdToUse + 2, afterBucketName );
                
                retval = BDBSharedDBBucketStore.createNewBucketStore(
                        txn, sharedDB, bucketId, firstIdToUse + 1, bucketName );
                prevStore.close();
                nextStore.close();
            } else {
                throw new AssertionError("Unrecognized bucket store type: " + bucketStorageType );
            }
            
            CurrentTransaction.getInstance(env).commitTransaction();
            
            openBucketStores.add( retval );
            return retval;
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        }  finally {
            if( CurrentTransaction.getInstance(env).getTransaction() != null ) {
                try {
                    CurrentTransaction.getInstance(env).abortTransaction();
                } catch (DatabaseException e) {
                    throw DatabaseUtil.translateBDBException(e);
                }
            }
        }
    }
    
    @Override
    public void closeBucketStore(BucketStore store) throws SeqStoreDatabaseException {
        if( store != null ) {
            ( ( BDBBackedBucketStore ) store ).close();
            openBucketStores.remove( store );
        }
    }
}
