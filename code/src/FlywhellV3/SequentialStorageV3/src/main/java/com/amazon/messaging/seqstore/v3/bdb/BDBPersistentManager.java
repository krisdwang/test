package com.amazon.messaging.seqstore.v3.bdb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import javax.measure.unit.NonSI;
import javax.measure.unit.Unit;

import lombok.Data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.bdb.jmx.BDBPersistenceStats;
import com.amazon.messaging.seqstore.v3.bdb.jmx.BDBPersistenceStatsMBean;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreAlreadyCreatedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDeleteInProgressException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData.BucketState;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.util.OpenBucketStoreTracker;
import com.amazon.messaging.utils.FileUtils;
import com.amazon.messaging.utils.Scheduler;
import com.sleepycat.bind.tuple.BooleanBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.StoredCollection;
import com.sleepycat.collections.StoredEntrySet;
import com.sleepycat.collections.StoredIterator;
import com.sleepycat.collections.StoredKeySet;
import com.sleepycat.collections.StoredSortedKeySet;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.TransactionStats;
import com.sleepycat.je.VersionMismatchException;
import com.sleepycat.util.RuntimeExceptionWrapper;

import edu.umd.cs.findbugs.annotations.CheckForNull;

/**
 * Used to keep track of buckets and stores that are created so they will not be
 * lost between restarts. This class takes advantage of BDB's transactionality
 * to do so.
 * 
 * @author stevenso, kaitchuc 
 */
public class BDBPersistentManager implements StorePersistenceManager {
    public static final String V3_SUBFOLDER_PATH = "/flywheel/messages.v3";
    
    public static final String V3_LONG_LIVED_MESSAGES_SUBFOLDER_PATH = "/flywheel/ll-messages.v3";
    
    /**
     * Set this System property to cause the manager to fail if any LLM features are used. This is used
     * for backwards compatibility testing only 
     */
    @TestOnly
    static final String FAIL_IF_LLM_FEATURES_USED_PROPERTY = 
            BDBPersistentManager.class.getName() + ".FailIfLLMFeatureUsed";
    
    /**
     * The number of buckets to mark deleted per transaction in {@link #markAllBucketsForStoreAsPendingDeletion(StoreId)}. 
     * This is public for unit tests.
     */
    public static final int BUCKETS_TO_MARK_DELETED_PER_TRANSACTION = 100;
    
    private static final Log log = LogFactory.getLog(BDBPersistentManager.class);
    
    private static final StatsConfig defaultStatsConfig;
    
    static {
        defaultStatsConfig = new StatsConfig();
        defaultStatsConfig.setFast(true);
    }
    
    @Data
    private static class PerformanceStatsData {
        private long previousCallTimeNanos;

        private EnvironmentStats previousStats;
    }
    
    /**
     * Returns true if the BDB has already been created on disk
     * at the location specified by config. This is done by checking if the
     * je.lck file exists in the {@link #V3_SUBFOLDER_PATH} below
     * {@link SeqStoreImmutablePersistenceConfig#getStoreDirectory()}
     */
    public static boolean bdbExists(SeqStoreImmutablePersistenceConfig persistConfig) {
        File dataDirectory = new File( persistConfig.getStoreDirectory(), V3_SUBFOLDER_PATH );
        File lockFile = new File( dataDirectory, "je.lck" );
        return lockFile.exists();
    }

    /**
     * Get the bucket name give the storeId and the bucketId for the bucket.
     */
    static String getBucketName(StoreId storeId, AckIdV3 bucketId) {
        return storeId.getStoreName() + "|" + bucketId.getTime() + "|" + bucketId.getSeq();
    }
    
    /**
     * Get a string that is the common prefix for all buckets in the given store. This
     * string will always be lexicographically before the names of all buckets for the 
     * store.
     */
    private static String getBucketNamePrefix(String storeName) {
        return storeName + "|";
    }

    /**
     * Get a string that will always be sorted after all buckets for the given store
     * but before any buckets for any following stores.
     */
    private static String getBucketNameTerminator(String storeName) {
        return storeName + "}"; // "}" follows "|" alphabetically...
    }

    private static class CheckpointTask implements Runnable {
        private final Environment environment;
        
        private final CheckpointConfig checkpointConfig;
        
        private CheckpointTask(Environment environment) {
            this.environment = environment;
            checkpointConfig = new CheckpointConfig();
            checkpointConfig.setMinutes(1);
        }
        
        @Override
        public void run() {
            try {
                environment.cleanLog();
                environment.compress();
                environment.checkpoint(checkpointConfig);
            } catch (Throwable e) {
                log.error("Checkpoint failed", e );
            }
        }
    }
    
    private final Environment mainEnvironment_;

    @CheckForNull
    private final Environment longLivedMessagesEnvironment_;
    
    private final CheckpointTask mainEnvCheckpointTask;
    
    private final CheckpointTask llMEnvCheckpointTask;
    
    private final SeqStoreImmutablePersistenceConfig config;

    private final Database ackLevelsDB;

    private final Database bucketMetadataDB;
    
    private final Database sequenceDB;
    
    private final Database sharedBucketStoreDB;
    
    private final Database storeDeletionInProgressDB;
    
    private final Sequence bucketIdSequence;

    private final StoredSortedMap<String, Map<String, AckIdV3>> ackLevels;
    
    private final StoredSortedMap<String, Boolean> storeDeletionsInProgress;

    private final StoredSortedMap<String, BucketPersistentMetaData> bucketMetadata;
    
    private final OpenBucketStoreTracker<BDBBackedBucketStore> openBucketStoreTracker;

    private final Scheduler commonThreadPool;
    
    private final boolean readOnly_;
    
    private final PerformanceStatsData mainPerformanceStatsData;
    
    private final PerformanceStatsData llmPerformanceStatsData;
    
    private final boolean useLongedLivedEnvironment;
    
    private final Set<BucketStorageType> supportedNewBucketTypes;
    
    // Used for unit testing 
    private final boolean supportLLMFeatures;

    static enum GlobalDatabaseNames {
        ACK_LEVELS,
        BUCKET_STORE,
        SEQUENCE_STORE,
        SHARED_BUCKET_STORE,
        STORE_DELETIONS_IN_PROGRESS
    }
    
    private static enum Sequences {
        BUCKET_ID
    }

    private boolean isClosed;

    private BDBPersistenceStatsMBean statsMBean;
    
    public BDBPersistentManager(SeqStoreImmutablePersistenceConfig config, Scheduler commonThreadPool)
            throws SeqStoreDatabaseException {
        this.config = config;
        this.commonThreadPool = commonThreadPool;
        isClosed = false;
        readOnly_ = config.isOpenReadOnly();
        openBucketStoreTracker = new OpenBucketStoreTracker<BDBBackedBucketStore>();
        useLongedLivedEnvironment = config.useSeperateEnvironmentForDedicatedBuckets();
        
        supportLLMFeatures = getSupportLLMFeatures();
        if( !supportLLMFeatures && useLongedLivedEnvironment ) {
            throw new IllegalArgumentException(
                    "Cannot set failIfLLMFeaturesUsed to true when useSeperateEnvironmentForDedicatedBuckets is also true");
        }
        
        commonEnvSetup(config);
        try {
            mainEnvironment_ = initBDBEnvironment(config, V3_SUBFOLDER_PATH, config.getMainJePropertiesFile());

            if( supportLLMFeatures && ( (!readOnly_ && useLongedLivedEnvironment) || llmEnvironmentExists( config ) ) ) {
                boolean success = false;
                try {
                    longLivedMessagesEnvironment_ = 
                            initBDBEnvironment(config, V3_LONG_LIVED_MESSAGES_SUBFOLDER_PATH, config.getLongLivedMessagesJePropertiesFile());
                    success = true;
                } finally {
                    if( !success ) {
                        try {
                            // Shutdown the main env if the llm env can't be loaded
                            mainEnvironment_.close();
                        } catch( Exception e ) {
                            // Don't hide the exception that caused opening the llm env to fail
                            log.warn( "Exception closing main environment", e );
                        }
                    }
                }
            } else {
                longLivedMessagesEnvironment_ = null;
            }
        } catch (EnvironmentNotFoundException e) {
            throw new SeqStoreDatabaseException( "Unable to find store at " + config.getStoreDirectory(), e );
        } catch( EnvironmentLockedException e ) {
            if( readOnly_ ) {
                throw new SeqStoreDatabaseException( 
                        "Unable to get lock on environment at " + config.getStoreDirectory() + "." +
                        " The environment may be busy with cleanup. Shutdown the writer and/or try again later.", e );
            } else {
                throw new SeqStoreDatabaseException( 
                        "Unable to get lock on environment at " + config.getStoreDirectory() + "." +
                        " Check for other processes writing to the same store.", e );
            }
        } catch (VersionMismatchException e) {
            throw new SeqStoreDatabaseException( "Store was written with newer BDB version: " + e.getMessage(), e );
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        }
        
        Set<BucketStorageType> tmpSupportedBucketTypes = EnumSet.of( BucketStorageType.DedicatedDatabase, BucketStorageType.SharedDatabase );
        
        if( useLongedLivedEnvironment ) {
            tmpSupportedBucketTypes.add(BucketStorageType.DedicatedDatabaseInLLMEnv);
        }
        
        supportedNewBucketTypes = Collections.unmodifiableSet(tmpSupportedBucketTypes);
        
        mainPerformanceStatsData = new PerformanceStatsData();
        mainPerformanceStatsData.setPreviousCallTimeNanos(System.nanoTime());
        mainPerformanceStatsData.setPreviousStats(getMainEnvironmentStats());
        
        if( longLivedMessagesEnvironment_ != null ) {
            llmPerformanceStatsData = new PerformanceStatsData();
            llmPerformanceStatsData.setPreviousCallTimeNanos(System.nanoTime());
            llmPerformanceStatsData.setPreviousStats(getLongLivedMessagesEnvironmentStats());
        } else {
            llmPerformanceStatsData = null; 
        }
        
        boolean success = false;
        try {
            ackLevelsDB = getGlobalDatabase( mainEnvironment_, GlobalDatabaseNames.ACK_LEVELS, readOnly_, false );
            preloadDatabase(ackLevelsDB);
            
            ackLevels = new StoredSortedMap<String, Map<String, AckIdV3>>(
                    ackLevelsDB, new StringBinding(), new ReaderToAckLevelEntryBinding(), true);
            
            ackLevels.getCursorConfig().setReadUncommitted(false);
            ackLevels.getCursorConfig().setReadCommitted(true);
            
            storeDeletionInProgressDB = getGlobalDatabase(
                    mainEnvironment_, GlobalDatabaseNames.STORE_DELETIONS_IN_PROGRESS, readOnly_, true);
            if( storeDeletionInProgressDB == null ) {
                storeDeletionsInProgress = null;
            } else {
                preloadDatabase(storeDeletionInProgressDB);
                
                // Do a consistency check for store that were recreated while the environment was rolled back
                // to a version that didn't know about storeDeletionsInProgress
                removeRecreatedStores( mainEnvironment_, ackLevelsDB, storeDeletionInProgressDB );
                
                storeDeletionsInProgress = new StoredSortedMap<String, Boolean>(
                        storeDeletionInProgressDB, new StringBinding(), new BooleanBinding(), !readOnly_);
                storeDeletionsInProgress.getCursorConfig().setReadCommitted(true);
                storeDeletionsInProgress.getCursorConfig().setReadUncommitted(false);
            }
            
            bucketMetadataDB = getGlobalDatabase(mainEnvironment_, GlobalDatabaseNames.BUCKET_STORE, readOnly_, false);
            
            int bdbBucketMetadataVersion;
            if( useLongedLivedEnvironment ) {
                bdbBucketMetadataVersion = 3;
            } else {
                bdbBucketMetadataVersion = 2;
            }
            
            bucketMetadata = new StoredSortedMap<String, BucketPersistentMetaData>(
                    bucketMetadataDB, new StringBinding(), 
                    new BucketPersistentMetaDataToEntryBinding(bdbBucketMetadataVersion, supportLLMFeatures), 
                    true);
            
            bucketMetadata.getCursorConfig().setReadUncommitted(false);
            bucketMetadata.getCursorConfig().setReadCommitted(true);
            
            if( !readOnly_ ) {
                // Sequences aren't needed in a read-only database and may not exist for older store formats
                sequenceDB = getGlobalDatabase(mainEnvironment_, GlobalDatabaseNames.SEQUENCE_STORE, false, false);
                
                bucketIdSequence = 
                        DatabaseUtil.createSequence(sequenceDB, Sequences.BUCKET_ID.toString(), true, 100 );
            } else {
                sequenceDB = null;
                bucketIdSequence = null;
            }

            sharedBucketStoreDB = getGlobalDatabase(
                    mainEnvironment_, GlobalDatabaseNames.SHARED_BUCKET_STORE, readOnly_, true);
            
            preloadDatabase(bucketMetadataDB);
            
            if( !readOnly_ ) {
                mainEnvCheckpointTask = new CheckpointTask(mainEnvironment_);
                commonThreadPool.executePeriodically( "Main Env Checkpoint task", mainEnvCheckpointTask, 1800000, false);
                
                if( longLivedMessagesEnvironment_ != null ) {
                    llMEnvCheckpointTask = new CheckpointTask(longLivedMessagesEnvironment_);
                    commonThreadPool.executePeriodically( "Long Lived Messages Env Checkpoint task", llMEnvCheckpointTask, 1800000, false);
                } else {
                    llMEnvCheckpointTask = null;
                }
            } else {
                mainEnvCheckpointTask = null;
                llMEnvCheckpointTask = null;
            }
            
            success = true;
        } catch(RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } finally {
            if( !success ) {
                try {
                    mainEnvironment_.close();
                } catch( IllegalStateException e ) {
                    // This is expected as we don't cleanly shutdown databases
                    log.info( "Got expected exception when shutting down main environment due to error: " + e.getMessage() );
                }
                
                if( longLivedMessagesEnvironment_ != null ) {
                    try {
                        longLivedMessagesEnvironment_.close();
                    } catch( IllegalStateException e ) {
                        // This is expected as we don't cleanly shutdown databases
                        log.info( "Got expected exception when shutting down llm environment due to error: " + e.getMessage() );
                    }
                }
            }
        }
    }

    private static boolean getSupportLLMFeatures() {
        boolean failIfLLMFeaturesUsed = false;
        String failIfLLMFeaturesUsedStr = System.getProperty(FAIL_IF_LLM_FEATURES_USED_PROPERTY);
        if( failIfLLMFeaturesUsedStr != null ) {
            failIfLLMFeaturesUsed = Boolean.parseBoolean(failIfLLMFeaturesUsedStr);
        }
        return !failIfLLMFeaturesUsed;
    }

    private static void removeRecreatedStores(
        Environment environment, Database ackLevelsDB, Database storeDeletionInProgressDB)
        throws SeqStoreDatabaseException
    {
        Transaction transaction = environment.beginTransaction(null, null);
        try {
            Cursor cursor = storeDeletionInProgressDB.openCursor(transaction, CursorConfig.READ_COMMITTED);
            try {
                DatabaseEntry keyEntry = new DatabaseEntry();
                // We don't care about the value
                DatabaseEntry valueEntry = new DatabaseEntry();
                valueEntry.setPartial(0, 0, true);
                
                OperationStatus status;
                for( status = cursor.getFirst(keyEntry, valueEntry, null);
                     status == OperationStatus.SUCCESS; 
                     status = cursor.getNext(keyEntry, valueEntry, null) )
                {
                    OperationStatus ackLevelsStatus = ackLevelsDB.get(transaction, keyEntry, valueEntry, null);
                    if( ackLevelsStatus != OperationStatus.NOTFOUND ) {
                        DatabaseUtil.checkSuccess(ackLevelsStatus, "get");
                        
                        // Delete the entry from the deletion in progress database as the store has been
                        //  recreated
                        cursor.delete();
                    }
                }
                
                if( status != OperationStatus.NOTFOUND ) {
                    throw new SeqStoreDatabaseException("Unexpected status from database " + status );
                }
            } finally {
                cursor.close();
            }
            
            transaction.commit();
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException("Error in removeRecreatedStores", e);
        } finally {
            // Abort the transaction if its still open
            if( transaction.isValid() ) transaction.abort();
        }
    }

    private static boolean rmDir(File dir) {
        if( !dir.exists() ) return true;
        
        File[] files = dir.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                rmDir(f);
            } else {
               if (! f.delete())
                   throw new IllegalStateException("Failed to delete file.");
            }
        }
        return dir.delete();
    }
    
    private static void commonEnvSetup(SeqStoreImmutablePersistenceConfig config)
            throws DatabaseException 
    {
        File storeDirectory = config.getStoreDirectory();
        if ((storeDirectory == null) || (storeDirectory.exists() && !storeDirectory.isDirectory())) {
            throw new IllegalArgumentException("null or invalid storeDirectory: " + storeDirectory);
        }
        
        File mainDir = new File(storeDirectory.getAbsolutePath(), V3_SUBFOLDER_PATH);
        File llmDir = new File(storeDirectory.getAbsolutePath(), V3_LONG_LIVED_MESSAGES_SUBFOLDER_PATH);

        log.info("Messages will be stored under " + mainDir + " and " + llmDir );
        
        if( config.isTruncateDatabase() ) {
            log.info("store directories will be removed prior to truncate");
            if (rmDir(mainDir) && mainDir.mkdirs()) {
                log.info("store directory " + mainDir + " was removed and reestablished");
            } else {
                log.warn("store directory " + mainDir + " could not be removed...continuing");
            }
            
            if (rmDir(llmDir) && llmDir.mkdirs()) {
                log.info("store directory " + llmDir + " was removed and reestablished");
            } else {
                log.warn("store directory " + llmDir + " could not be removed...continuing");
            }
        }
        
        // Ensure that the BDB JE mbean is available
        System.setProperty("JEMonitor", "true");
    }
    
    private static boolean llmEnvironmentExists(SeqStoreImmutablePersistenceConfig config) {
        File dataDirectory = new File(config.getStoreDirectory(), V3_LONG_LIVED_MESSAGES_SUBFOLDER_PATH);
        File lockFile = new File( dataDirectory, "je.lck" );
        return lockFile.exists();
    }

    private Environment initBDBEnvironment(SeqStoreImmutablePersistenceConfig config, String relativePath, File jePropertiesFile)
            throws DatabaseException 
    {
        File storeDirectory = new File(config.getStoreDirectory().getAbsolutePath(), relativePath);

        log.info("Setting up message store in: " + storeDirectory);
        
        if( !storeDirectory.exists() && !storeDirectory.mkdirs() ) {
            throw new IllegalStateException( storeDirectory.getAbsolutePath() + " does not exist and cannot be created" );
        } else if( !storeDirectory.isDirectory() ) {
            throw new IllegalStateException( storeDirectory.getAbsolutePath() + " is not a directory" );
        }

        if( jePropertiesFile != null ) {
            copyJePropertiesFile( jePropertiesFile, storeDirectory );
        }
        
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(config.isOpenReadOnly());
        envConfig.setAllowCreate(!config.isOpenReadOnly()); // if environment does not exist when
        envConfig.setTransactional(true);
        envConfig.setLockTimeout(5, TimeUnit.SECONDS);
        envConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
        envConfig.setConfigParam(EnvironmentConfig.ENV_DB_EVICTION, "true");
        envConfig.setConfigParam(EnvironmentConfig.TXN_DUMP_LOCKS, "true");
        envConfig.setSharedCache(true);

        return createBDBEnvironment(storeDirectory, envConfig);
    }
    
    /**
     * Create the bdb environment. This is a separate function so unit tests can override it
     */
    Environment createBDBEnvironment(File storeDirectory, EnvironmentConfig envConfig) {
        return new Environment(storeDirectory, envConfig);
    }

    private static void copyJePropertiesFile(File jePropertiesFile, File storeDirectory) {
        if( !jePropertiesFile.exists() ) {
            log.warn( "je properties file " + jePropertiesFile + " does not exist." );
            return;
        }
        
        if( !jePropertiesFile.isFile() ) {
            log.warn( "je properties file " + jePropertiesFile.getAbsolutePath() + " is not a file." );
            return;
        }
        
        if( !jePropertiesFile.canRead() ) {
            log.warn( "je properties file " + jePropertiesFile.getAbsolutePath() + " is not readable." );
            return;
        }
        
        File destFile = new File( storeDirectory, "je.properties" );
        
        try {
            FileUtils.copyFile(jePropertiesFile, destFile);
        } catch( IOException ex ) {
            log.warn("Failed to copy je.properties file from " + jePropertiesFile.getAbsolutePath() + " to " +
                     destFile.getAbsolutePath(), ex);
        }
    }
    
    private static Database getGlobalDatabase(
        Environment env, GlobalDatabaseNames databaseName, 
        boolean readOnly, boolean allowMissingIfReadOnly) 
        throws DatabaseException
    {
        try {
            return DatabaseUtil.createDB(
                    env, databaseName.toString(), true, false, readOnly );
        } catch( DatabaseNotFoundException e ) {
            // Allow read-only stores to be loaded even if they don't have the deletions in progress db
            if( !allowMissingIfReadOnly || !readOnly ) throw e;
            return null;
        }
    }
    
    private static void preloadDatabase(Database bucketMetadataDB) throws SeqStoreDatabaseException {
        try {
            // Preload the data into the cache. This is faster than just reading
            //  as it can read object in disk order instead of key order. 
            PreloadConfig preLoadConfig = new PreloadConfig();
            preLoadConfig.setLoadLNs(true);
            bucketMetadataDB.preload(preLoadConfig);
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException("Error getting database statistics.", ex);
        }
    }
    
    @Override
    public boolean isHealthy() {
        return !isClosed && mainEnvironment_.isValid() && 
                ( longLivedMessagesEnvironment_ == null || longLivedMessagesEnvironment_.isValid() );
    }
    
    private BucketPersistentMetaData startLLMEnvDedicatedBucketCreation(String bucketName, AckIdV3 bucketId) 
            throws SeqStoreDatabaseException 
    {
        assert longLivedMessagesEnvironment_ != null;
        
        CurrentTransaction mainEnvCurTransaction = CurrentTransaction.getInstance(mainEnvironment_);
        CurrentTransaction llmEnvCurTransaction = CurrentTransaction.getInstance(longLivedMessagesEnvironment_);
        
        boolean success = false;
        
        assert mainEnvCurTransaction.getTransaction() == null;
        assert llmEnvCurTransaction.getTransaction() == null;
        
        long bucketSequenceId;
        BucketPersistentMetaData metadata;
        
        TransactionConfig tconfig = new TransactionConfig();
        tconfig.setReadCommitted(true);
        Transaction mainEnvTxn = mainEnvCurTransaction.beginTransaction(tconfig);
        try {
            mainEnvTxn.setName(Thread.currentThread().getName() + ":mainEnvCreateBucketMetadata");
            
            bucketSequenceId = bucketIdSequence.get(null, 1);
            
            metadata = new BucketPersistentMetaData(
                    bucketSequenceId, bucketId, BucketStorageType.DedicatedDatabaseInLLMEnv, BucketPersistentMetaData.BucketState.CREATING );
            BucketPersistentMetaData oldMetadata = bucketMetadata.putIfAbsent(bucketName, metadata );
            if( oldMetadata != null ) {
                if( oldMetadata.getBucketState() == BucketState.CREATING && oldMetadata.getBucketId().equals( bucketId ) ) {
                    // Abort but don't throw as this is probably an attempt to resume creation of previously failed bucket creation
                    mainEnvCurTransaction.abortTransaction();
                    metadata = oldMetadata;
                } else {
                    throw new IllegalStateException("Bucket " + bucketName + " already exists" );
                }
            } else {
                mainEnvCurTransaction.commitTransaction();
            }
            success = true;
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        } finally {
            if( !success ) {
                try {
                    mainEnvCurTransaction.abortTransaction();
                } catch (Exception e1) {
                    // Just log the exception as we only get here if there was an earlier exception and we don't
                    //  want to hide that one
                    log.error( "Error aborting transaction" , e1 );
                }
            }
        }
        
        return metadata;
    }
    
    /**
     * Finish the creation of a dedicated bucket store in the llm environent. 
     * This function must be called with an open transaction on mainEnvironment_ and will 
     * change the metadata for the bucket to active on success. It will not commit 
     * the mainEnvironment_ transaction. This function can be called repeatedly for the
     * same bucket without errors so that it can be called again if the commit
     * of the main transaction fails or by multiple threads racing to create/load the
     * bucket.
     * <p> 
     * Not private so unit test can override it
     */
    BDBBackedBucketStore finishLLMEnvDedicatedBucketCreation( 
        String bucketName, BucketPersistentMetaData metadata ) 
            throws SeqStoreDatabaseException 
    {
        assert longLivedMessagesEnvironment_ != null;
        assert metadata.getBucketState() == BucketPersistentMetaData.BucketState.CREATING;
        
        if( CurrentTransaction.getInstance(mainEnvironment_).getTransaction() == null ) {
            throw new IllegalStateException("finishLongLivedBucketStoreCreation must be called with a mainEnvironment_ transaction open");
        }
        
        CurrentTransaction longEnvCurTransaction = CurrentTransaction.getInstance(longLivedMessagesEnvironment_);
        assert longEnvCurTransaction.getTransaction() == null;
        
        BDBBackedBucketStore bucketStore = null;
        
        TransactionConfig tconfig = new TransactionConfig();
        tconfig.setReadCommitted(true);
        Transaction longEnvTxn = longEnvCurTransaction.beginTransaction(tconfig);
        
        boolean success = false;
        try {
            longEnvTxn.setName(Thread.currentThread().getName() + ":llEnvFinishCreateBucket");
            
            AckIdV3 bucketId = metadata.getBucketId();
            long bucketSequenceId = metadata.getBucketSequenceId();
            
            BucketPersistentMetaData activeMetadata = metadata.copyWithNewState(BucketPersistentMetaData.BucketState.ACTIVE);
            if( !bucketMetadata.replace( bucketName, metadata, activeMetadata) ) {
                // Another thread created the bucket first
                BucketPersistentMetaData currentMetadata = bucketMetadata.get(bucketName);
                
                assert currentMetadata.getBucketId().equals(bucketId) && currentMetadata.getBucketSequenceId() == bucketSequenceId;
                
                if( currentMetadata.isDeleted() ) {
                    throw new IllegalStateException( "Bucket " + bucketName + " is already deleted" );
                }
                
                assert currentMetadata.isActive();
                
                bucketStore = 
                        BDBBucketStore.getOrCreateBucketStore(
                                longLivedMessagesEnvironment_, longEnvTxn,
                                BucketStorageType.DedicatedDatabaseInLLMEnv,
                                bucketId, bucketSequenceId, bucketName, readOnly_, false);
            } else {
                // Create (or load if the DB already exists) the database for the bucket
                bucketStore = BDBBucketStore.getOrCreateBucketStore(
                        longLivedMessagesEnvironment_, longEnvTxn, 
                        BucketStorageType.DedicatedDatabaseInLLMEnv,
                        bucketId, bucketSequenceId, bucketName, readOnly_, true);
            }
           
            // Must commit the long env transaction first.
            longEnvCurTransaction.commitTransaction();
            success = true;
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        } finally {
            if( !success ) {
                if( bucketStore != null ) {
                    try {
                        bucketStore.close();
                    } catch( Throwable e1 ) {
                        // Just log the exception as we only get here if there was an earlier exception and we don't
                        //  want to hide that one
                        log.error( "Error closing bucket store", e1 );
                    }
                }
                
                try {
                    longEnvCurTransaction.abortTransaction();
                } catch (Exception e1) {
                    // Just log the exception as we only get here if there was an earlier exception and we don't
                    //  want to hide that one
                    log.error( "Error aborting transaction" , e1 );
                }
            }
        }
        
        return bucketStore;
    }
    
    private BucketStore createDedicatedBucketInLLMEnv( StoreId destinationId, AckIdV3 bucketId ) throws SeqStoreDatabaseException 
    {
        String bucketName = getBucketName(destinationId, bucketId);
        BucketPersistentMetaData metadata = startLLMEnvDedicatedBucketCreation( bucketName, bucketId );
        
        // At this point the buckets existence has been persisted. If the process dies or the transaction fails after this 
        //  point the create will resume after the restart, or on the next attempt to use the bucket.
        BDBBackedBucketStore bucketStore = null;
        boolean success = false;
        
        CurrentTransaction mainEnvCurTransaction = CurrentTransaction.getInstance(mainEnvironment_);
        assert mainEnvCurTransaction.getTransaction() == null;
        
        TransactionConfig tconfig = new TransactionConfig();
        tconfig.setReadCommitted(true);
        Transaction mainEnvTxn = mainEnvCurTransaction.beginTransaction(tconfig);
        try {
            mainEnvTxn.setName(Thread.currentThread().getName() + ":mainEnvFinishCreateBucket");
            bucketStore = finishLLMEnvDedicatedBucketCreation(bucketName, metadata);
            mainEnvCurTransaction.commitTransaction();
            success = true;
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        } finally {
            if( !success ) {
                if( bucketStore != null ) {
                    try {
                        bucketStore.close();
                    } catch( Throwable e1 ) {
                        // Just log the exception as we only get here if there was an earlier exception and we don't
                        //  want to hide that one
                        log.error( "Error closing bucket store", e1 );
                    }
                }
                
                try {
                    mainEnvCurTransaction.abortTransaction();
                } catch (Exception e1) {
                    // Just log the exception as we only get here if there was an earlier exception and we don't
                    //  want to hide that one
                    log.error( "Error aborting transaction" , e1 );
                }
            }
        }
        
        openBucketStoreTracker.addBucketStore(destinationId, bucketStore);
        return bucketStore;
    }
    
    @Override
    public BucketStore createBucketStore(
        StoreId destinationId, AckIdV3 bucketId, BucketStorageType storageType)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        if( storageType == BucketStorageType.DedicatedDatabaseInLLMEnv ) {
            if( useLongedLivedEnvironment ) {
                return createDedicatedBucketInLLMEnv(destinationId, bucketId);
            } else {
                storageType = BucketStorageType.DedicatedDatabase;
            }
        }
        
        return createBucketInMainEnv(destinationId, bucketId, storageType);
    }

    private BucketStore createBucketInMainEnv(
        StoreId destinationId, AckIdV3 bucketId, BucketStorageType storageType)
        throws SeqStoreDatabaseException
    {
        String bucketName = getBucketName(destinationId, bucketId);
        
        CurrentTransaction mainEnvCurTransaction = CurrentTransaction.getInstance(mainEnvironment_);
        
        BDBBackedBucketStore bucketStore = null;
        boolean success = false;
        
        assert mainEnvCurTransaction.getTransaction() == null;
        
        TransactionConfig tconfig = new TransactionConfig();
        tconfig.setReadCommitted(true);
        Transaction mainEnvTxn = mainEnvCurTransaction.beginTransaction(tconfig);
        try {
            mainEnvTxn.setName(Thread.currentThread().getName() + ":mainEnvCreateBucketStore");
            
            long bucketSequenceId;
            // Don't use the transaction as its better to have the concurrency than to
            //  protect against wasting sequence ids
            bucketSequenceId = bucketIdSequence.get(null, 1);

            // Start as active as the create of both store and metadata is done in one transaction
            BucketPersistentMetaData metadata = 
                    new BucketPersistentMetaData(bucketSequenceId, bucketId, storageType, BucketPersistentMetaData.BucketState.ACTIVE);
            if( bucketMetadata.putIfAbsent(bucketName, metadata ) != null ) {
                throw new IllegalStateException("Bucket " + bucketName + " already exists" );
            }
            
            bucketStore = createBucketStore(
                    destinationId, bucketId, bucketSequenceId, bucketName, true, 
                    metadata.getBucketStorageType(), mainEnvTxn);
            mainEnvCurTransaction.commitTransaction();
            
            success = true;
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        } finally {
            if( !success ) {
                if( bucketStore != null ) {
                    try {
                        bucketStore.close();
                    } catch( SeqStoreDatabaseException e1 ) {
                        // Just log the exception as we only get here if there was an earlier exception and we don't
                        //  want to hide that one
                        log.error( "Error closing bucket store", e1 );
                    }
                }
                
                try {
                    mainEnvCurTransaction.abortTransaction();
                } catch (Exception e1) {
                    // Just log the exception as we only get here if there was an earlier exception and we don't
                    //  want to hide that one
                    log.error( "Error aborting transaction" , e1 );
                }
            }
        }
            
        openBucketStoreTracker.addBucketStore(destinationId, bucketStore);
        return bucketStore;
    }
    
    @Override
    public BucketStore getBucketStore(StoreId destinationId, AckIdV3 bucketId)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        String bucketName = getBucketName(destinationId, bucketId);
        
        Transaction txn = null;
        CurrentTransaction mainEnvCurTransaction = CurrentTransaction.getInstance(mainEnvironment_);
        
        BDBBackedBucketStore bucketStore = null;
        boolean success = false;
        try {
            assert mainEnvCurTransaction.getTransaction() == null;
            
            TransactionConfig tconfig = new TransactionConfig();
            tconfig.setReadCommitted(true);
            
            txn = mainEnvCurTransaction.beginTransaction(tconfig);
            txn.setName(Thread.currentThread().getName() + ":getBucketStore");
            
            BucketPersistentMetaData metadata = bucketMetadata.get(bucketName);
            if( metadata == null || ( readOnly_ && metadata.isCreating() ) ) {
                throw new IllegalStateException("Bucket " + bucketName + " does not exist");
            } else if( metadata.isDeleted() ) {
                throw new IllegalStateException( "Bucket " + bucketName + " is already deleted" );
            }
            
            if( metadata.isCreating() ) {
                assert metadata.getBucketStorageType() == BucketStorageType.DedicatedDatabaseInLLMEnv;
                
                // The bucket was being created by another thread, or was partially created prior to a crash. 
                // Finish the creation here on demand as there is enough information available to so safely
                bucketStore = finishLLMEnvDedicatedBucketCreation(bucketName, metadata);
            } else {
                bucketStore = createBucketStore(
                        destinationId, bucketId, metadata.getBucketSequenceId(), bucketName, false, 
                        metadata.getBucketStorageType(), txn);
            }
            mainEnvCurTransaction.commitTransaction();
            
            success = true;
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        } finally {
            if (!success ) {
                if( bucketStore != null ) {
                    try {
                        bucketStore.close();
                    } catch( SeqStoreDatabaseException e ) {
                        log.info( "Error closing bucket store while cleanup up from earlier error", e );
                    }
                }
                
                if( txn != null ) {
                    try {
                        mainEnvCurTransaction.abortTransaction();
                    } catch (Exception e1) {
                        log.info( "Error aborting transaction", e1 );
                    }
                }
            }
        }
        
        openBucketStoreTracker.addBucketStore(destinationId, bucketStore);
        return bucketStore;
    }
    
    @Override
    public void closeBucketStore(StoreId storeId, BucketStore bucketStore) 
            throws SeqStoreDatabaseException 
    {
        BDBBackedBucketStore store;
        try {
            store = ( BDBBackedBucketStore ) bucketStore;
        } catch( ClassCastException e ) {
            throw new IllegalArgumentException(
                    "BucketStores can only be closed by the persistence manager that created them");
        }
        
        openBucketStoreTracker.removeBucketStore(storeId, store);
        store.close();
    }
    
    private BDBBackedBucketStore createBucketStore(
        StoreId storeId, AckIdV3 bucketId, long bucketSequenceId, String bucketName,
        boolean newBucket, BucketStorageType bucketStorageType, Transaction mainEnvTxn) 
        throws SeqStoreDatabaseException 
    {
        BDBBackedBucketStore retval;
        
        switch( bucketStorageType ) {
        case DedicatedDatabaseInLLMEnv:
            assert longLivedMessagesEnvironment_ != null;
            if( newBucket != false ) {
                throw new IllegalArgumentException(
                        "This function does not support creating new buckets in the long lived database env");
            }
            retval = BDBBucketStore.getOrCreateBucketStore(
                    longLivedMessagesEnvironment_, null, bucketStorageType, bucketId, bucketSequenceId, bucketName, readOnly_, newBucket);
            break;
        case DedicatedDatabase:
            retval = BDBBucketStore.getOrCreateBucketStore(
                    mainEnvironment_, mainEnvTxn, bucketStorageType, bucketId, bucketSequenceId, bucketName, readOnly_, newBucket);
            break;
        case SharedDatabase:
            if( newBucket ) {
                retval = BDBSharedDBBucketStore.createNewBucketStore(
                        mainEnvTxn, sharedBucketStoreDB, bucketId, bucketSequenceId, bucketName);
            } else {
                retval = BDBSharedDBBucketStore.loadBucketStore(
                        sharedBucketStoreDB, bucketId, bucketSequenceId, bucketName);
            }
            break;
        default:
            throw new IllegalArgumentException("Unrecognized bucket storage type: " + bucketStorageType ); 
        }
        
        return retval;
    }
    
    @Override
    public boolean deleteBucketStore(StoreId storeId, AckIdV3 bucketId)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        String bucketName = getBucketName(storeId, bucketId);
        if( openBucketStoreTracker.hasOpenStoreForBucket(storeId, bucketId) ) {
            throw new IllegalStateException(bucketName + " is still open");
        }
        
        CurrentTransaction currentMainEnvTxn = CurrentTransaction.getInstance(mainEnvironment_);
        Transaction mainEnvTxn = null;
        TransactionConfig tconfig = new TransactionConfig();
        tconfig.setReadCommitted(true);
        
        try {
            assert currentMainEnvTxn.getTransaction() == null;
            mainEnvTxn = currentMainEnvTxn.beginTransaction(tconfig);
            mainEnvTxn.setName(Thread.currentThread().getName() + ":deleteBucket");

            BucketPersistentMetaData metadata = bucketMetadata.remove(bucketName);
            if( metadata == null ) {
                return false;
            }
            
            switch( metadata.getBucketStorageType() ) {
            case DedicatedDatabaseInLLMEnv:
            {
                if( longLivedMessagesEnvironment_ == null ) {
                    throw new SeqStoreDatabaseException(
                            "Bucket is marked as dedicated bucket in the llm environment but" +
                    		"the llm environment doesn't exist");
                }
                
                CurrentTransaction currentLLEnvTxn = CurrentTransaction.getInstance(longLivedMessagesEnvironment_);
                Transaction llEnvTxn = currentLLEnvTxn.beginTransaction(tconfig);
                try {
                    deleteDedicatedBucketStore(longLivedMessagesEnvironment_, llEnvTxn, bucketName, metadata);
                    currentLLEnvTxn.commitTransaction();
                } finally {
                    if( llEnvTxn.isValid() ) currentLLEnvTxn.abortTransaction();
                }
                break;
            }
            case DedicatedDatabase:
                deleteDedicatedBucketStore( mainEnvironment_, mainEnvTxn, bucketName, metadata );
                break;
            case SharedDatabase:
                deleteSharedBucketStore(bucketName, mainEnvTxn, metadata);
                break;
            default:
                throw new IllegalStateException("Unrecognized bucket store type " + metadata.getBucketStorageType() );
            }
            
            currentMainEnvTxn.commitTransaction();
        } catch (DatabaseException e) {
            log.error("Could not delete bucket: ", e);
            throw DatabaseUtil.translateBDBException(e);
        } catch (RuntimeExceptionWrapper e) {
            log.error("Could not delete bucket: ", e);
            throw DatabaseUtil.translateWrappedBDBException(e);
        } finally {
            if( currentMainEnvTxn.getTransaction() != null ) {
                currentMainEnvTxn.abortTransaction();
            }
        }
        
        return true;
    }

    /**
     * Delete a shared bucket store. This is its own method so unit tests can override it
     */
    void deleteSharedBucketStore(
        String bucketName, Transaction mainEnvTxn, BucketPersistentMetaData metadata)
        throws SeqStoreDatabaseException
    {
        BDBSharedDBBucketStore.deleteBucketStore(mainEnvTxn, sharedBucketStoreDB, bucketName, metadata.getBucketSequenceId());
    }

    /**
     * Delete a dedicated bucket store. This is its own method so unit tests can override it
     */
    void deleteDedicatedBucketStore(Environment env, Transaction txn, String bucketName, BucketPersistentMetaData metadata )
        throws SeqStoreDatabaseException
    {
        BDBBucketStore.deleteBucketStore(env, txn, bucketName, metadata.getBucketSequenceId());
    }
    
    private void checkForOpenBuckets(StoreId storeId, StoredCollection<BucketPersistentMetaData> bucketSet) 
            throws SeqStoreDatabaseException 
    {
        StoredIterator<BucketPersistentMetaData> itr = bucketSet.storedIterator(false);
        try {
            while( itr.hasNext() ) {
                if( openBucketStoreTracker.hasOpenStoreForBucket(storeId, itr.next().getBucketId()) ) {
                    throw new IllegalStateException( "Store " + storeId + " still has open buckets." );
                }
            }
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        } finally {
            itr.close();
        }
    }
    
    @Override
    public boolean storeDeletionStarted(StoreId storeId) throws SeqStoreDatabaseException {
        if( storeDeletionsInProgress == null ) {
            // storeDeletionsInProgress is only allowed to be null if read-only
            throw new SeqStoreDatabaseException("Attempt to delete using a read-only environment");
        }
        
        try {
            String storeName = storeId.getStoreName();
            TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setReadCommitted(true);
            
            CurrentTransaction currentTransaction = CurrentTransaction.getInstance(mainEnvironment_);
            Transaction txn = currentTransaction.beginTransaction(txnConfig);
            try {
                txn.setName("StoreDeletionStarted-"+storeName);
                
                Map<String, AckIdV3> storeAckLevels = ackLevels.remove(storeName);
                if( storeAckLevels == null ) {
                    if( log.isDebugEnabled() ) {
                        log.debug( "storeDeletionStarted called for store that was already being deleted");
                    }
                    return false; 
                }
                
                Boolean existingValue = storeDeletionsInProgress.putIfAbsent(storeName, true);
                if( existingValue != null ) {
                    // This shouldn't be possible but it doesn't break anything
                    log.error( "Found entry in storeDeletionsInProgress for " + storeId + " when it was also in ackLevels");
                }
                
                currentTransaction.commitTransaction();
                return true;
            } finally {
                if( currentTransaction.getTransaction() != null ) {
                    currentTransaction.abortTransaction();
                }
            }
            
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        }
    }
    
    @Override
    public void storeDeletionCompleted(StoreId storeId) throws SeqStoreDatabaseException {
        if( storeDeletionsInProgress == null ) {
            throw new SeqStoreDatabaseException("Attempt to delete using a read-only store");
        }
        
        try {
            String storeName = storeId.getStoreName();
            
            CurrentTransaction currentTransaction = CurrentTransaction.getInstance(mainEnvironment_);
            Transaction txn = currentTransaction.beginTransaction(null);
            try {
                txn.setName("StoreDeletionCompletedTxn-" + storeName);
                
                Boolean markedAsPending = storeDeletionsInProgress.remove(storeName);
                if( markedAsPending == null ) {
                    if( log.isDebugEnabled() ) {
                        log.debug( "Not deleting " + storeName + " as it was recreated or already fully deleted");
                    }
                    currentTransaction.abortTransaction();
                    return;
                }
                
                if( hasBuckets(storeName) ) {
                    throw new IllegalStateException(
                            "Attempt to mark deletion of " + storeName + " as complete while there are still buckets remaining");
                }
                
                txn.commit();
            } finally {
                if( currentTransaction.getTransaction() != null ) {
                    currentTransaction.abortTransaction();
                }
            }
            
            
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        }
    }
    
    @Override
    public void deleteStoreWithNoBuckets(StoreId storeId) throws SeqStoreDatabaseException {
        try {
            String storeName = storeId.getStoreName();
            
            CurrentTransaction currentTransaction = CurrentTransaction.getInstance(mainEnvironment_);
            Transaction txn = currentTransaction.beginTransaction(null);
            try {
                txn.setName("DeleteStoreWithNoBucketsTxn-" + storeName);
                
                if( hasBuckets(storeName) ) {
                    throw new IllegalStateException(
                            "deleteStoreWithNoBuckets called for " + storeName + " while there are still buckets remaining");
                }
                
                Map<String, AckIdV3> storedAckLevels = ackLevels.remove(storeName);
                if( storedAckLevels == null ) {
                    if( log.isDebugEnabled() ) {
                        log.debug( "deleteStoreWithNoBuckets called for store " + storeName + " that was already deleted");
                    }
                    return;
                }
                
                txn.commit();
            } finally {
                if( currentTransaction.getTransaction() != null ) {
                    currentTransaction.abortTransaction();
                }
            }
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        }
    }
    
    @Override
    public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(StoreId storeId)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        String storeName = storeId.getStoreName();
        
        String startingId = getBucketNamePrefix(storeName);
        String endingId = getBucketNameTerminator(storeName);
        
        try {
            StoredSortedMap<String, BucketPersistentMetaData> bucketMetadataForStore =
                    (StoredSortedMap<String, BucketPersistentMetaData>) bucketMetadata.subMap(
                            startingId, endingId);
            
            checkForOpenBuckets( 
                    storeId, 
                    (StoredCollection<BucketPersistentMetaData>) bucketMetadataForStore.values() );
            
            CurrentTransaction currentTransaction = CurrentTransaction.getInstance(mainEnvironment_);
            assert currentTransaction.getTransaction() == null;
            
            String transactionBaseName = Thread.currentThread().getName() + ":deleteStore-" + storeId.getStoreName();
            
            
            TransactionConfig tconfig = new TransactionConfig();
            tconfig.setReadCommitted(true);
            
            List<BucketPersistentMetaData> retval = new ArrayList<BucketPersistentMetaData>();
            int deleteBatch = 0;
            boolean done = false;
            while( !done ) {
                StoredSortedMap<String, BucketPersistentMetaData> unmarkedBuckets =
                        (StoredSortedMap<String, BucketPersistentMetaData>) bucketMetadata.subMap(
                                startingId, false, endingId, false );
                
                Transaction txn = currentTransaction.beginTransaction(tconfig);
                try {
                    txn.setName(transactionBaseName + ":" + ( deleteBatch++ ) );
                    
                    @SuppressWarnings("unchecked")
                    StoredIterator<Map.Entry<String, BucketPersistentMetaData>> itr = 
                            ((StoredEntrySet<String, BucketPersistentMetaData>) unmarkedBuckets.entrySet()).storedIterator(true);
                    try {
                        String lastKey = null;
                        for( int i = 0; i < BUCKETS_TO_MARK_DELETED_PER_TRANSACTION && itr.hasNext(); ++i ) {
                            Map.Entry<String, BucketPersistentMetaData> entry = itr.next();
                            BucketPersistentMetaData oldMetadata = entry.getValue();
                            if( !oldMetadata.isDeleted() ) {
                                entry.setValue(oldMetadata.getDeletedMetadata());
                            }

                            retval.add( entry.getValue() );
                            lastKey = entry.getKey();
                        }
                        
                        done = !itr.hasNext();
                        if( !done ) {
                            assert lastKey != null;
                            startingId = lastKey;
                        }
                    } finally {
                        itr.close();
                    }
                
                    currentTransaction.commitTransaction();
                } finally {
                    if( currentTransaction.getTransaction() != null ) {
                        currentTransaction.abortTransaction();
                    }
                }
            }
            
            return Collections.unmodifiableList( retval );
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        }
    }
    
    @Override
    public boolean hasBuckets(StoreId storeId) throws SeqStoreDatabaseException {
        return hasBuckets( storeId.getStoreName() );
    }
    
    private boolean hasBuckets(String storeName) throws SeqStoreDatabaseException {
        try {
            return !bucketMetadata.subMap(
                            getBucketNamePrefix(storeName),
                            getBucketNameTerminator(storeName)).isEmpty();
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        }
    }
    
    @Override
    public void prepareForClose() {
        if( !isClosed && !readOnly_ ) {
            try {
                disableBDBCleanupThreads();
            } catch( SeqStoreClosedException e ) {
                // Shouldn't happen but if it does there is nothing to prepare for.
            }
        }
    }

    @Override
    public void close() throws SeqStoreDatabaseException, SeqStoreInternalException {
        if (!isClosed) {
            shutdownBackgroundTasks();
            closeEnvironment();
        }
    }
    
    /**
     * Close the environment and all the remaniaing open databases.
     * @return true if successful, false if there was an error that may have caused the shutdown not to work
     */
    private boolean closeEnvironment() {
        try{
            boolean hadFailure = !closeOpenBuckets();
            
            try {
                bucketMetadataDB.close();
                ackLevelsDB.close();
                if( sequenceDB != null ) {
                    bucketIdSequence.close();
                    sequenceDB.close();
                }
                if( sharedBucketStoreDB != null ) sharedBucketStoreDB.close();
                if( storeDeletionInProgressDB != null ) storeDeletionInProgressDB.close();
            } catch (Exception e) {
                log.error("Failed closing metadata databases", e);
                hadFailure = true;
            }
            
            try {
                mainEnvironment_.close();
            } catch( DatabaseException e ) {
                log.error("Had error closing main enviroment", e);
                hadFailure = true;
            } catch( IllegalStateException e ) {
                log.error("Had error closing main enviroment", e);
                hadFailure = true;
            }
            
            if( longLivedMessagesEnvironment_ != null ) {
                try {
                    longLivedMessagesEnvironment_.close();
                } catch( DatabaseException e ) {
                    log.error("Had error closing llm enviroment", e);
                    hadFailure = true;
                } catch( IllegalStateException e ) {
                    log.error("Had error closing llm enviroment", e);
                    hadFailure = true;
                }
            }
            
            return !hadFailure;
        } finally {
            // No matter what consider the environment closed after this function is called
            isClosed = true;
        }
    }

    /**
     * Close the open buckets.
     * @return true if successful, false if there was an error that may have caused the shutdown not to work
     */
    private boolean closeOpenBuckets() {
        boolean hadFailure = false;
        
        if( openBucketStoreTracker.openBucketStoreCount() != 0 ) {
            log.warn( openBucketStoreTracker.openBucketStoreCount() + " buckets stores were left open.");
        }

        for( BDBBackedBucketStore store : openBucketStoreTracker.getOpenBucketStores() ) {
            try {
                store.close();
            } catch (SeqStoreDatabaseException ex) {
                log.error("Failed closing bucket store " + 
                   store.getSequenceId() + "-" + store.getBucketId() + ":" + ex.getMessage(), ex);
                hadFailure = true;
            }
        }
        openBucketStoreTracker.clear();
        
        return !hadFailure;
    }

    private void shutdownBackgroundTasks() {
        if( !readOnly_ ) {
            commonThreadPool.cancel( mainEnvCheckpointTask, false, true);
            if( llMEnvCheckpointTask != null ) {
                commonThreadPool.cancel( llMEnvCheckpointTask, false, true);
            }
        }
    }
    
    @Override
    public Collection<BucketPersistentMetaData> getBucketMetadataForStore(StoreId storeId)
        throws SeqStoreDatabaseException
    {
        try {
            String storeName = storeId.getStoreName();
            StoredSortedMap<String, BucketPersistentMetaData> bucketsForStore = 
                (StoredSortedMap<String, BucketPersistentMetaData>) bucketMetadata.subMap(
                    getBucketNamePrefix(storeName),
                    getBucketNameTerminator(storeName));
            
            List<BucketPersistentMetaData> retval = new ArrayList<BucketPersistentMetaData>();
            StoredIterator<BucketPersistentMetaData> itr = 
                    ((StoredCollection<BucketPersistentMetaData> ) bucketsForStore.values()).storedIterator(false);
            try {
                while( itr.hasNext() ) {
                    BucketPersistentMetaData metadata = itr.next();
                    if( !readOnly_  || !metadata.isCreating() ) {
                        // Don't count partially created buckets in a read only environment
                        retval.add( metadata );
                    }
                }
            } finally {
                itr.close();
            }
            
            return Collections.unmodifiableCollection( retval );
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException("Failed recovering buckets", e);
        } catch( RuntimeExceptionWrapper e ) {
            throw DatabaseUtil.translateWrappedBDBException("Failed recovering buckets", e);
        }
    }
    
    public EnvironmentStats getMainEnvironmentStats() throws SeqStoreDatabaseException {
        try { 
            return mainEnvironment_.getStats(defaultStatsConfig);
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException("Error getting database statistics.", ex);
        } catch( RuntimeExceptionWrapper e ) {
            throw DatabaseUtil.translateWrappedBDBException("Error getting database statistics", e);
        }
    }
    
    public EnvironmentStats getLongLivedMessagesEnvironmentStats() throws SeqStoreDatabaseException {
        // return an empty stats object
        if( longLivedMessagesEnvironment_ == null ) {
            return new EnvironmentStats();
        }
        
        try {
            return longLivedMessagesEnvironment_.getStats(defaultStatsConfig);
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException("Error getting database statistics.", ex);
        } catch( RuntimeExceptionWrapper e ) {
            throw DatabaseUtil.translateWrappedBDBException("Error getting database statistics", e);
        }
    }
    
    @Override
    public int getNumOpenBuckets() {
        return openBucketStoreTracker.openBucketCount();
    }
    
    @Override
    public int getNumOpenDedicatedBuckets() {
        return openBucketStoreTracker.openDedicatedBucketCount();
    }
    
    @Override
    public int getNumBuckets() throws SeqStoreDatabaseException {
        try {
            return (int) bucketMetadataDB.count();
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException("Error getting number of buckets.", e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        }
    }
    
    @Override
    public int getNumDedicatedBuckets() throws SeqStoreDatabaseException {
        try {
            int count = 0;
            // Loop through the entries in bucketMetadataDB using the database instead of 
            //  the map so that the cache mode and the READ_UNCOMMITTED can be specified
            //  so that the operation is as cheap as possible and as much as possible 
            //  does not block on other threads
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            Cursor cursor = bucketMetadataDB.openCursor(null, cursorConfig);
            try {
                // Don't force the data into the cache if it isn't already there
                cursor.setCacheMode(CacheMode.UNCHANGED); 
                DatabaseEntry valueEntry = new DatabaseEntry();
                DatabaseEntry keyEntry = new DatabaseEntry();
                while( cursor.getNext( keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS ) {
                    BucketPersistentMetaData metadata = 
                            BucketPersistentMetaDataToEntryBinding.deserialize(
                                    new TupleInput(valueEntry.getData(), valueEntry.getOffset(), valueEntry.getSize()), 
                                    supportLLMFeatures);
                    if( metadata.getBucketStorageType().isDedicated() ) {
                        count++;
                    }
                } 
            } finally {
                cursor.close();
            }
            
            return count;
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException("Error getting number of dedicated buckets.", e);
        }
    }
    
    // test use only
    @TestOnly
    @Override
    public void printDBStats() throws SeqStoreDatabaseException {
        try {
            System.out.println("Main Environment:");
            System.out.println("==================================================");
            printEnvStats(mainEnvironment_);
            
            if( longLivedMessagesEnvironment_ != null ) {
                System.out.println("Long Lived Messages Environment:");
                System.out.println("==================================================");
                printEnvStats(longLivedMessagesEnvironment_);            
            }
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException("Error getting database statistics.", ex);
        }
    }

    private static void printEnvStats(Environment env) {
        EnvironmentStats stats = env.getStats(null);
        System.out.println("Cache used for holding data and keys:" + stats.getDataBytes());
        System.out.println("Total je cache in use: " + stats.getCacheTotalBytes());
        System.out.println("Total cache used for locks and transactions: " + stats.getLockBytes());

        TransactionStats transactionStats = env.getTransactionStats(null);
        System.out.println("Total active transactions: " + transactionStats.getNActive());
        for (TransactionStats.Active trans : transactionStats.getActiveTxns()) {
            if( trans != null ) { // Somehow a null transaction can get into the list of active transactions
                System.out.println(
                        "\tId=" + trans.getId() + ", name=" + trans.getName() + 
                        ", parentId=" + trans.getParentId());
            }
        }

        StatsConfig statConfig = new StatsConfig();
        statConfig.setFast(false);
        System.out.println("Total read locks held: " + stats.getNReadLocks());
        System.out.println("Total write locks held: " + stats.getNWriteLocks());
        System.out.println("Total waiters: " + stats.getNWaiters());
    }
    
    @TestOnly
    public BucketPersistentMetaData getPersistedBucketData(StoreId storeId, AckIdV3 bucketId) throws SeqStoreDatabaseException {
        try {
            return bucketMetadata.get( getBucketName(storeId, bucketId) );
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException("Error getting database statistics.", e);
        } catch( RuntimeExceptionWrapper e ) {
            throw DatabaseUtil.translateWrappedBDBException("Error getting database statistics.", e);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "{ config= " + config + "}";
    }
    
    @Override
    public void createStore(StoreId storeId, Map<String, AckIdV3> initialAckLevels)
        throws SeqStoreDatabaseException, SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException
    {
        if( storeDeletionsInProgress == null ) {
            // storeDeletionsInProgress is only allowed to be null if read-only
            throw new SeqStoreDatabaseException("Attempt to create a store using a read-only environment");
        }
        
        Map<String, AckIdV3> safeCopy = Collections.unmodifiableMap( new HashMap<String, AckIdV3>(initialAckLevels) );
        String storeName = storeId.getStoreName();
        
        try {
            CurrentTransaction currentTransaction = CurrentTransaction.getInstance(mainEnvironment_);
            Transaction txn = currentTransaction.beginTransaction(null);
            try {
                txn.setName("CreateStoreTxn-" + storeName);
                
                if( storeDeletionsInProgress.containsKey(storeName) ) {
                    throw new SeqStoreDeleteInProgressException( "Store " + storeId + " is still being deleted.");
                }
                
                // This will block any parallel creates or deletes until the transaction completes
                if( ackLevels.putIfAbsent(storeId.getStoreName(), safeCopy) != null ) {
                    throw new SeqStoreAlreadyCreatedException( "createStore called for store " + storeId + " that already exists" );
                }
                
                currentTransaction.commitTransaction();
            } finally {
                if( currentTransaction.getTransaction() != null ) {
                    currentTransaction.abortTransaction();
                }
            }
        } catch(DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException("Failed persisting ack levels", e);
        }
    }

    @Override
    public void persistReaderLevels(StoreId storeId, Map<String, AckIdV3> storeAckLevels) throws SeqStoreDatabaseException {
        try {
            Map<String, AckIdV3> safeCopy = Collections.unmodifiableMap( new HashMap<String, AckIdV3>(storeAckLevels) );
            if( ackLevels.replace(storeId.getStoreName(), safeCopy) == null ) {
                throw new IllegalArgumentException( "persistReaderLevels called for store " + storeId + " that doesn't exist" );
            }
        } catch(DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e) {
            throw DatabaseUtil.translateWrappedBDBException("Failed persisting ack levels", e);
        }
    }
    
    @Override
    public Map<String, AckIdV3> getReaderLevels( StoreId storeId ) throws SeqStoreDatabaseException {
        try {
            return ackLevels.get( storeId.getStoreName() );
        } catch(DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e ) {
            throw DatabaseUtil.translateWrappedBDBException( "Failed getting ack levels", e);
        }
    }

    @Override
    public Object getStatisticsMBean() {
        if( statsMBean == null ) {
            statsMBean = new BDBPersistenceStats(this);
        }
        return statsMBean;
    }
    
    /*
     * These functions are for use by StoreUtil
     */
    
    /**
     * Get a set of all StoreIds. This set is backed by the BDB stored set
     * @throws SeqStoreDatabaseException 
     */
    @Override
    public Set<StoreId> getStoreIds() throws SeqStoreDatabaseException {
        try {
            Set<StoreId> storeIds = new HashSet<StoreId>();
            copyKeysToStoreIdCollection( ackLevels, storeIds);
            return Collections.unmodifiableSet(storeIds);
        } catch(DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e ) {
            throw DatabaseUtil.translateWrappedBDBException(e);
        }
    }
    
    @Override
    public Set<StoreId> getStoreIdsForGroup(String group) throws SeqStoreDatabaseException {
        Set<StoreId> storeIdsForGroup = new HashSet<StoreId>();
        String escapedGroup = StoreIdImpl.escapeGroup( group );
        if( ackLevels.containsKey( escapedGroup ) ) {
            storeIdsForGroup.add( new StoreIdImpl( group, null ) );
        }
        
        String groupWithIdStart = escapedGroup + ":";
        StoredSortedKeySet<String> ackLevelsKeySet = ( StoredSortedKeySet<String> ) ackLevels.keySet();
        StoredIterator<String> itr = 
            ( ( StoredSortedKeySet<String> ) ackLevelsKeySet.tailSet( groupWithIdStart ) ).storedIterator(false);
        try {
            while( itr.hasNext() ) {
                String storeName = itr.next();
                if( !storeName.startsWith( groupWithIdStart ) ) break;
                storeIdsForGroup.add( new StoreIdImpl( storeName ) );
            }
        } finally {
            itr.close();
        }
        
        return storeIdsForGroup;
    }
    
    @Override
    public Set<String> getGroups() {
        TreeSet<String> groups = new TreeSet<String>();

        StoredIterator<String> itr = ((StoredKeySet<String>) ackLevels.keySet()).storedIterator(false);
        try {
            while (itr.hasNext()) {
                String storeName = itr.next();
                String group = StoreIdImpl.unescapeGroup(StoreIdImpl.getEscapedGroupFromStoreName(storeName));
                groups.add( group );
            }
        } finally {
            itr.close();
        }

        return Collections.unmodifiableSet(groups);
    }
    
    @Override
    public boolean containsStore( StoreId storeId ) throws SeqStoreDatabaseException {
        try {
            return ackLevels.containsKey( storeId.getStoreName() );
        } catch(DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper e ) {
            throw DatabaseUtil.translateWrappedBDBException("Failed recovering stores", e);
        }
    }
    
    @Override
    public boolean updateBucketMetadata(
        StoreId destinationId, BucketPersistentMetaData metadata)
        throws SeqStoreDatabaseException
    {
        String bucketName = getBucketName(destinationId, metadata.getBucketId());
        
        try {
            // Replace the old metadata only if it exists and the bucket isn't marked as pending deletion
            BucketPersistentMetaData oldMetadata = bucketMetadata.get(bucketName);
            while( true ) {
                if( oldMetadata == null ) return false;
                if( oldMetadata.isDeleted() ) return false;
                if( bucketMetadata.replace(bucketName, oldMetadata, metadata) ) return true;
                oldMetadata = bucketMetadata.get(bucketName);
            }
        } catch(DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        } catch( RuntimeExceptionWrapper ex ) {
            throw DatabaseUtil.translateWrappedBDBException( "Failed flushing metadata", ex );
        }
    }
    
    @Override
    public SeqStoreImmutablePersistenceConfig getConfig() {
        return config;
    }
    
    private static long getTotalEnvSize(File dir) {
        long totalSize = 0;
        for( File file : dir.listFiles() ) {
            if( file.getName().endsWith(".jdb") ) {
                totalSize += file.length();
            }
        }
        
        return totalSize;
    }
    
    private static long getTotalEnvSize(Environment env) {
        return getTotalEnvSize(env.getHome());
    }
    
    @Override
    public void reportPerformanceMetrics(Metrics metrics) throws SeqStoreDatabaseException {
        metrics.addCount("OpenBuckets", getNumOpenBuckets(), Unit.ONE);
        metrics.addCount("OpenDedicatedBuckets", getNumOpenDedicatedBuckets(), Unit.ONE);
        metrics.addCount("SeqStores", ackLevels.size(), Unit.ONE );
        
        long mainSize = getTotalEnvSize(mainEnvironment_);
        long llSize = 0;
        if( longLivedMessagesEnvironment_ != null ) {
            llSize += getTotalEnvSize(longLivedMessagesEnvironment_);
        }
        metrics.addCount("bdbSize", mainSize + llSize, NonSI.BYTE);
        metrics.addCount("mainBdbSize", mainSize, NonSI.BYTE);
        metrics.addCount("llBdbSize", llSize, NonSI.BYTE);
        
        long currentTime = System.nanoTime();
        
        EnvironmentStats stats;
        EnvironmentStats oldStats;
        long oldTime;
        synchronized (mainPerformanceStatsData) {
            oldStats = mainPerformanceStatsData.getPreviousStats();
            oldTime = mainPerformanceStatsData.getPreviousCallTimeNanos();
            
            stats = getMainEnvironmentStats();
            
            mainPerformanceStatsData.setPreviousStats( stats );
            mainPerformanceStatsData.setPreviousCallTimeNanos(currentTime);
        }
        
        reportStats("", currentTime, oldTime, stats, oldStats, metrics);
        reportStats("main-", currentTime, oldTime, stats, oldStats, metrics);
        
        
        log.info( "BDB Main EnvironmentStats:\n" + stats );
        
        if( longLivedMessagesEnvironment_ != null ) {
            currentTime = System.nanoTime();
            synchronized (llmPerformanceStatsData) {
                oldStats = llmPerformanceStatsData.getPreviousStats();
                oldTime = llmPerformanceStatsData.getPreviousCallTimeNanos();
                
                stats = getLongLivedMessagesEnvironmentStats();
                
                llmPerformanceStatsData.setPreviousStats( stats );
                llmPerformanceStatsData.setPreviousCallTimeNanos(currentTime);
            }
            
            reportStats("", currentTime, oldTime, stats, oldStats, metrics);
            reportStats("ll-", currentTime, oldTime, stats, oldStats, metrics);
            
            log.info( "BDB Long Lived Messages EnvironmentStats:\n" + stats );
        }
        
    }

    private static void reportStats(
        String prefix, long currentTime, long oldTime, EnvironmentStats stats, EnvironmentStats oldStats, Metrics metrics)
    {
        metrics.addCount(prefix + "cacheSize", stats.getCacheTotalBytes(), NonSI.BYTE);
        metrics.addCount(prefix + "dataCacheSize", stats.getDataBytes(), NonSI.BYTE);
        metrics.addCount(prefix + "lockAndTransactionCacheSize", stats.getCacheTotalBytes(), NonSI.BYTE);
        metrics.addCount(prefix + "logCacheSize", stats.getBufferBytes(), NonSI.BYTE);
        metrics.addCount(prefix + "adminCacheSize", stats.getAdminBytes(), NonSI.BYTE);
        metrics.addCount(prefix + "cleanerBacklog", stats.getCleanerBacklog(), Unit.ONE );
        metrics.addCount(prefix + "fileDeletionBacklog", stats.getFileDeletionBacklog(), Unit.ONE );
        
        // Skip rate metrics the first time
        double timeSinceLastCallInSeconds = 
            ( currentTime - oldTime ) / 1000000000.;
        if( timeSinceLastCallInSeconds > 0 ) { // This should always be true
            metrics.addCount(prefix + "randomReadsPerSecond", 
                    ( stats.getNRandomReads() - oldStats.getNRandomReads() ) / timeSinceLastCallInSeconds, 
                    Unit.ONE );
            
            metrics.addCount(prefix + "randomWritesPerSecond", 
                    ( stats.getNRandomWrites() - oldStats.getNRandomWrites() ) / timeSinceLastCallInSeconds, 
                    Unit.ONE );
            
            metrics.addCount(prefix + "sequentialReadsPerSecond", 
                    ( stats.getNSequentialReads() - oldStats.getNSequentialReads() ) / timeSinceLastCallInSeconds, 
                    Unit.ONE );
            
            metrics.addCount(prefix + "sequentialWritesPerSecond", 
                    ( stats.getNSequentialWrites() - oldStats.getNSequentialWrites() ) / timeSinceLastCallInSeconds, 
                    Unit.ONE );
            
            metrics.addCount(prefix + "randomReadBytesPerSecond", 
                    ( stats.getNRandomReadBytes() - oldStats.getNRandomReadBytes() ) / timeSinceLastCallInSeconds, 
                    NonSI.BYTE );
            
            metrics.addCount(prefix + "randomWriteBytesPerSecond", 
                    ( stats.getNRandomWriteBytes() - oldStats.getNRandomWriteBytes() ) / timeSinceLastCallInSeconds, 
                    NonSI.BYTE );
            
            metrics.addCount(prefix + "sequentialReadBytesPerSecond", 
                    ( stats.getNSequentialReadBytes() - oldStats.getNSequentialReadBytes() ) / timeSinceLastCallInSeconds, 
                    NonSI.BYTE );
            
            metrics.addCount(prefix + "sequentialWriteBytesPerSecond", 
                    ( stats.getNSequentialWriteBytes() - oldStats.getNSequentialWriteBytes() ) / timeSinceLastCallInSeconds, 
                    NonSI.BYTE );
            
            metrics.addCount(prefix + "criticalEvictionBatchesPerSecond",
                    ( stats.getNBatchesCritical() - oldStats.getNBatchesCritical() ) / timeSinceLastCallInSeconds,
                    Unit.ONE );
            
            metrics.addCount(prefix + "daemonEvictionBatchesPerSecond",
                    ( stats.getNBatchesDaemon() - oldStats.getNBatchesDaemon() ) / timeSinceLastCallInSeconds,
                    Unit.ONE );
            
            metrics.addCount(prefix + "evictorThreadEvictionBatchesPerSecond",
                    ( stats.getNBatchesEvictorThread() - oldStats.getNBatchesEvictorThread() ) / timeSinceLastCallInSeconds,
                    Unit.ONE );
            
            metrics.addCount(prefix + "fsyncsPerSecond", 
                    ( stats.getNFSyncs() - oldStats.getNFSyncs() ) / timeSinceLastCallInSeconds,
                    Unit.ONE );
            
            // These metrics should be near 0 - if they're not we need to tune the BDB
            metrics.addCount(prefix + "repeatFaultReadsPerSecond",
                    ( stats.getNRepeatFaultReads() - oldStats.getNRepeatFaultReads() ) / timeSinceLastCallInSeconds,
                    Unit.ONE );
            
            metrics.addCount(prefix + "tmpBufferWritesPerSecond",
                    ( stats.getNTempBufferWrites() - oldStats.getNTempBufferWrites() ) / timeSinceLastCallInSeconds,
                    Unit.ONE );
        }
    }
    
    /**
     * Get the number of records in the database used for shared database buckets. This is an expensive call 
     * for use in unit tests only. It is only guaranteed to be accurate if no operations are in progress.
     *  
     * @return the number of of records in the database used for shared database buckets
     * @throws SeqStoreDatabaseException 
     */
    @TestOnly
    public long getSharedDatabaseSize() throws SeqStoreDatabaseException {
        try {
            if( sharedBucketStoreDB == null ) return 0;
            return sharedBucketStoreDB.count();
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException(e);
        }
    }
    
    @TestOnly
    Database getSharedBucketStoreDB() {
        return sharedBucketStoreDB;
    }
    
    /**
     * Disable automatic BDB cleanup. Once this is called it cannot be 
     * restarted and cleanup can only be done manually by calling {@link #runBDBCleanup()}.
     * 
     * @throws SeqStoreClosedException if the manager has already been closed
     */
    public void disableBDBCleanupThreads() throws SeqStoreClosedException {
        if( isClosed ) throw new SeqStoreClosedException("Environment has already been closed");
        
        if( !readOnly_ ) {
            // Cancel the checkpoint task as close will do a checkpoint
            commonThreadPool.cancel( mainEnvCheckpointTask, false, false);
            commonThreadPool.cancel( llMEnvCheckpointTask, false, false);
            
            disableCleanup(mainEnvironment_);
            if(longLivedMessagesEnvironment_ != null ) {
                disableCleanup(longLivedMessagesEnvironment_);
            }
        }
    }

    private static void disableCleanup(Environment env) {
        EnvironmentMutableConfig bdbConfig = env.getMutableConfig();
        if( !bdbConfig.getConfigParam(EnvironmentConfig.ENV_RUN_CLEANER).equals("false" ) ) {
            // Turn off the cleaner thread
            bdbConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
            env.setMutableConfig(bdbConfig);
        }
    }
    
    public void runBDBCleanup() throws SeqStoreDatabaseException {
        try {
            log.info( "Starting BDB Cleanup");
            Environment envs[];
            if (longLivedMessagesEnvironment_ != null ) {
                envs = new Environment[] { longLivedMessagesEnvironment_, mainEnvironment_ };
            } else {
                envs = new Environment[] { mainEnvironment_ }; 
            }
                    
            int logsCleaned;
            do {
                logsCleaned = 0;
                for( Environment env : envs ) {
                    int curLogsCleaned = env.cleanLog();
                    log.info( "Cleaned " + curLogsCleaned + " curLogsCleaned from " + env.getHome() );
                    logsCleaned += curLogsCleaned;
                    if( curLogsCleaned > 0 ) {
                        CheckpointConfig ckptConfig = new CheckpointConfig();
                        ckptConfig.setForce(true);
                        ckptConfig.setMinimizeRecoveryTime(true);
                        env.checkpoint(ckptConfig);
                    }
                }
            } while( logsCleaned > 0 );
            log.info( "Finished BDB Cleanup");
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException(e);
        }
    }

    @Override
    public Set<StoreId> getStoreIdsBeingDeleted() throws SeqStoreDatabaseException {
        Set<StoreId> storeIds = new HashSet<StoreId>();
        copyKeysToStoreIdCollection( storeDeletionsInProgress, storeIds );
        return Collections.unmodifiableSet( storeIds );
    }
    
    @SuppressWarnings("unchecked")
    private static void copyKeysToStoreIdCollection( StoredSortedMap<String, ?> storedMap, Collection<StoreId> dest) {
        copyToStoreIdCollection( ( StoredCollection<String> ) storedMap.keySet(), dest ); 
    }
    
    /**
     * Copy from a stored collection to a normal collection. This is more efficient than dest.addAll
     * as it does it with one cursor instead of doing chunks of records.
     * 
     * @param storedCollection
     * @param dest
     */
    private static void copyToStoreIdCollection( StoredCollection<String> storedCollection, Collection<StoreId> dest) {
        StoredIterator<String> itr = storedCollection.storedIterator(false);
        try {
            while( itr.hasNext() ) {
                dest.add( new StoreIdImpl( itr.next() ) );
            }
        } finally {
            itr.close();
        }
    }
    
    @Override
    public Set<BucketStorageType> getSupportedNewBucketTypes() {
        return supportedNewBucketTypes;
    }
    
    @TestOnly
    Environment getMainEnvironment() {
        return mainEnvironment_;
    }
    
    @TestOnly
    Environment getLongLivedMessagesEnvironment() {
        return longLivedMessagesEnvironment_;
    }
}
