package com.amazon.messaging.seqstore.v3.store;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.impls.SystemClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.AckId;
import com.amazon.messaging.seqstore.v3.SeqStoreMetrics;
import com.amazon.messaging.seqstore.v3.StoredCount;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.bdb.BDBPersistentManager;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.BucketStorageManager;
import com.amazon.messaging.seqstore.v3.internal.DefaultAckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.utils.Scheduler;

/**
 * Helper class used by StoreUtils.
 * 
 * @author stevenso
 */
public class StoreUtilHelper {
    private static final Log log = LogFactory.getLog(StoreUtilHelper.class);

    public static interface StoreUtilsMessageVisitor {

        public void visitMessage(StoredEntry<? extends AckId> entry) throws IOException;
    }

    private final Map<StoreId, Store> loadedStores = new HashMap<StoreId, Store>();

    private final BDBPersistentManager manager;
    
    private final BucketStorageManager bucketStorageManager;
    
    private final ConfigProvider<?> configProvider;
    
    private final AckIdGenerator ackIdGenerator;
    
    private final StoreSpecificClock clock;
    
    public StoreUtilHelper(File databaseDir, boolean readOnly) 
            throws SeqStoreException 
    {
        this( databaseDir, readOnly, new SystemClock(), null );
    }
    
    public StoreUtilHelper(File databaseDir, boolean readOnly, 
                           Clock clock, ConfigProvider<?> configProvider) 
            throws SeqStoreException 
    {
        SeqStorePersistenceConfig persistConfig = new SeqStorePersistenceConfig();
        persistConfig.setStoreDirectory(databaseDir);
        persistConfig.setTruncateDatabase(false);
        persistConfig.setOpenReadOnly(readOnly);
        // If it already exists it will be used but don't create and start using it if it doesn't exist.
        persistConfig.setUseSeperateEnvironmentForDedicatedBuckets(false);

        manager = new BDBPersistentManager(
                persistConfig.getImmutableConfig(), Scheduler.getGlobalInstance());
        
        bucketStorageManager = new BucketStorageManager(manager, Scheduler.getGlobalInstance(), new NullMetricsFactory());
        
        this.configProvider = configProvider;
        
        this.clock = new StoreSpecificClock( new AlwaysIncreasingClock( clock ) );
        
        this.ackIdGenerator = new AckIdGenerator();
    }

    public synchronized void close() throws SeqStoreException {
        for( Store store : loadedStores.values() ) {
            store.close();
        }
        
        bucketStorageManager.shutdown();
        manager.close();
    }

    public boolean storeExists(String storeName) throws SeqStoreDatabaseException {
        return manager.containsStore(new StoreIdImpl(storeName));
    }

    public boolean readerExists(String storeName, String readerName) throws SeqStoreDatabaseException {
        Map<String, AckIdV3> readerLevels = manager.getReaderLevels(new StoreIdImpl(storeName));
        return readerLevels != null && readerLevels.containsKey(readerName);
    }

    public Set<String> getStoreNames() throws SeqStoreDatabaseException {
        Set<String> retval = new HashSet<String>();
        for (StoreId storeID : manager.getStoreIds()) {
            retval.add(storeID.getStoreName());
        }

        return retval;
    }

    public Set<String> getReaderNames(String storeName) throws SeqStoreException {
        Map<String, AckIdV3> readerLevels = manager.getReaderLevels(new StoreIdImpl(storeName));
        if (readerLevels == null)
            return null;

        return readerLevels.keySet();
    }

    public synchronized void createStore(String storeName) throws SeqStoreException {
        // Do nothing if the store already exists
        if (storeExists(storeName))
            return;

        manager.persistReaderLevels(new StoreIdImpl(storeName), Collections.<String, AckIdV3> emptyMap());
    }

    public synchronized void createReader(String storeName, String reader) throws SeqStoreException {
        StoreId storeId = new StoreIdImpl(storeName);
        if (!storeExists(storeName))
            throw new IllegalArgumentException("Store " + storeName + " does not exist.");

        Map<String, AckIdV3> readerLevels = new HashMap<String, AckIdV3>(manager.getReaderLevels(storeId));

        if (readerLevels.containsKey(reader))
            return; // Reader already exists

        readerLevels.put(reader, AckIdV3.MINIMUM);
        manager.persistReaderLevels(storeId, readerLevels);
    }

    @SuppressWarnings("deprecation")
    public synchronized void deleteMessages(String storeName, long startTime, long endTime) throws SeqStoreException {
        AckIdV3 startAckId = new AckIdV3(startTime, false);
        AckIdV3 endAckId = new AckIdV3(endTime, false);
        StoreId storeId = new StoreIdImpl(storeName);
        Map<String, AckIdV3> readerLevels = manager.getReaderLevels(storeId);
        if (readerLevels == null)
            throw new IllegalArgumentException("Store " + storeId + " does not exist.");

        for (Map.Entry<String, AckIdV3> entry : readerLevels.entrySet()) {
            if (startAckId.compareTo(entry.getValue()) > 0) {
                throw new UnsupportedOperationException(
                        "Delete from the middle of a store is not supported. Reader " + entry.getKey() +
                                " has an ack level lower than " + (new Date(startTime)).toGMTString());
            }
        }

        Map<String, AckIdV3> newLevels = new HashMap<String, AckIdV3>();
        for (Map.Entry<String, AckIdV3> entry : readerLevels.entrySet()) {
            newLevels.put(entry.getKey(), AckIdV3.max(entry.getValue(), endAckId));
        }
        manager.persistReaderLevels(storeId, newLevels);
    }
    
    public synchronized long visitMessages(String storeName, long startTime, final long endTime,
                                           long maxMessages, StoreUtilsMessageVisitor visitor)
            throws IOException, SeqStoreException
    {
        StoreId storeId = new StoreIdImpl(storeName);

        Store store = getStore(storeId);
        StoreIterator itr = store.getIterAt(new AckIdV3(startTime, false));

        long count = 0;
        StoredEntry<AckIdV3> entry = null;
        while (count < maxMessages ) {
            entry = itr.next();
            if( entry == null || entry.getAckId().getTime() > endTime ) {
                break;
            }
            visitor.visitMessage(entry);
            count++;
        }
        return count;
    }

    public String getPrimary(String destination) {
        return new StoreIdImpl(destination).getGroupName();
    }

    public synchronized long getUnackedMessageCount(String storeName, String readerName)
            throws SeqStoreException
    {
        StoreIdImpl storeId = new StoreIdImpl(storeName);

        Store store = getStore(storeId);

        AckIdV3 readerLevel = manager.getReaderLevels(storeId).get(readerName);
        if (readerLevel == null)
            throw new IllegalArgumentException("Reader " + readerName + " does not exist.");

        StoreIterator itr = store.getIterAt(readerLevel);

        return store.getCountAfter(itr).getEntryCount();
    }

    /**
     * Get a store for the given storeId.
     * 
     * @param storeId
     *            the id of the store to fetch
     * @return the store for StoreId
     * @throws SeqStoreException
     *             if there is an error getting the store
     */
    private Store getStore(StoreId storeId) throws SeqStoreException {
        Store store = loadedStores.get( storeId );
        if( store != null ) return store;
        
        if (!manager.containsStore(storeId))
            throw new IllegalArgumentException("Store " + storeId + " does not exist");

        SeqStoreConfig config = new SeqStoreConfig();
        config.setCleanerPeriod(Long.MAX_VALUE);
        config.setGuaranteedRetentionPeriod(-1);
        config.setMaxMessageLifeTime(-1);
        
        store = new Store(
                storeId, bucketStorageManager, config.getImmutableConfig(), 
                new DefaultAckIdSourceFactory( ackIdGenerator, clock), clock );
        
        loadedStores.put( storeId, store );
        return store;
    }

    public synchronized void removeDestination(String storeName) throws SeqStoreException {
        StoreIdImpl storeId = new StoreIdImpl(storeName);
        if (!manager.containsStore(storeId))
            return; // Nothing to do

        Store store = loadedStores.remove( storeId );
        if( store != null ) {
            store.close();
        }
        
        // TODO: Do this in the background using bucketStoreManager
        if( !manager.hasBuckets( storeId ) ) {
            manager.deleteStoreWithNoBuckets(storeId);
        } else {
            manager.storeDeletionStarted(storeId);
            Collection<BucketPersistentMetaData> buckets = 
                    manager.markAllBucketsForStoreAsPendingDeletion(storeId);
            for( BucketPersistentMetaData metadata : buckets ) {
                manager.deleteBucketStore(storeId, metadata.getBucketId());
            }
            manager.storeDeletionCompleted(storeId);
        }
    }

    public synchronized void removeReader(String storeName, String reader) throws SeqStoreException {
        StoreIdImpl storeId = new StoreIdImpl(storeName);
        if (!storeExists(storeName))
            throw new IllegalArgumentException("Store " + storeName + " does not exist.");

        Map<String, AckIdV3> readerLevels = new HashMap<String, AckIdV3>(manager.getReaderLevels(storeId));

        if (!readerLevels.containsKey(reader))
            return; // Reader already exists

        readerLevels.remove(reader);
        manager.persistReaderLevels(storeId, readerLevels);
    }

    public long getStoredCount(String storeName) throws SeqStoreException {
        Store store = getStore(new StoreIdImpl(storeName));
        return store.getStoreMetrics().getTotalMessageCount().getEntryCount();
    }
    
    public BucketJumper getBucketJumper(String storeName, long startTime)  throws SeqStoreException {
        Store store = getStore(new StoreIdImpl(storeName));
        return store.getBucketJumperAt( new AckIdV3(startTime, false ) );
    }
    
    public SeqStoreMetrics getStoreMetrics(String storeName) throws SeqStoreException {
        Store store = getStore(new StoreIdImpl(storeName));
        return store.getStoreMetrics();
    }
    
    public SeqStoreMetrics getReaderMetrics(String storeName, String readerName) throws SeqStoreException {
        if (!storeExists(storeName))
            throw new IllegalArgumentException("Store " + storeName + " does not exist.");
        
        Map<String, AckIdV3> readerLevels = manager.getReaderLevels(new StoreIdImpl(storeName));
        if( !readerLevels.containsKey( readerName ) ) 
            throw new IllegalArgumentException("Reader " + readerName + " does not exist." );
        
        Store store = getStore(new StoreIdImpl(storeName));
        StoreIterator itr = store.getIterAt( readerLevels.get( readerName ) );
        
        StoredCount totalCount = store.getCountAfter(itr);
        StoredCount delayCount = store.getDelayedCount();
        long oldestMessageLogicalAge = 0;

        StoredEntry<AckIdV3> nextEntry = itr.next();
        if( nextEntry != null ) {
            oldestMessageLogicalAge = nextEntry.getAvailableTime() - store.getCurrentStoreTime();
        }

        return new SeqStoreMetrics (totalCount, delayCount, oldestMessageLogicalAge, 0.0, 0.0);
    }
    
    public void cleanup() throws SeqStoreException {
        Future<?> reporterFuture = null;

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        try {
            reporterFuture = executor.scheduleWithFixedDelay( new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println( new Date().toString() + "- Main BDB Stats: " +  manager.getMainEnvironmentStats() );
                        System.out.println( new Date().toString() + "- LL BDB Stats: " +  manager.getLongLivedMessagesEnvironmentStats() );
                    } catch( SeqStoreDatabaseException e ) {
                        log.error( "Failed reporting metrics", e );
                    } catch( Throwable e ) {
                        log.error( "Failed reporting metrics", e );
                    }
                } },
                1, 1, TimeUnit.MINUTES );

            if( configProvider == null ) {
                // Only BDB cleanup can be run as the config might specify a guaranteed retention period that would  
                //  keep all messages on disk
                manager.runBDBCleanup();
                return;
            }
            
            // BDB has an issue where cleanup threads can conflict with bucket deletion so turn them all off
            //  before doing the bulk delete
            manager.disableBDBCleanupThreads();

            // Run cleanup multiple times until cleanup completes quickly enough that it's unlikely a significant
            // number of messages reached their maximum retention period while cleanup was in progress. The later 
            // cleanup passes should be faster, because they should be deleting fewer messages. Still, limit 
            // the total number of passes to 5 so that cleanup always completes eventually.
            for( int i = 0; i < 5; ++i ) {
                long startTime = System.nanoTime();
                cleanupStores();
                manager.runBDBCleanup();
                long duration = System.nanoTime() - startTime;
                if( TimeUnit.NANOSECONDS.toMinutes( duration ) < 2 ) {
                    break;
                } else {
                    log.info( "Re-running cleanup as the last pass took more than 2 minutes");
                }
            }
        } finally {
            if( reporterFuture != null ) reporterFuture.cancel(false);
            executor.shutdown();
        }
    }
    
    @TestOnly
    Map<String, AckIdV3> getReaderLevels(String storeName) throws SeqStoreDatabaseException {
        return manager.getReaderLevels(new StoreIdImpl(storeName));
    }

    private void cleanupStores() throws SeqStoreDatabaseException, SeqStoreUnrecoverableDatabaseException {
        for( String group : manager.getGroups() ) {
            SeqStoreImmutableConfig groupConfig;
            try {
                groupConfig = configProvider.getStoreConfig(group);
            } catch (ConfigUnavailableException e) {
                log.warn( "Could not find config for " + group, e );
                continue;
            }
            
            long guaranteed = groupConfig.getGuaranteedRetentionPeriod();
            long maximum = groupConfig.getMaxMessageLifeTime();
            
            // config validation should have required the guaranteed is always greater than maximum while 
            // taking into account < 0 meaning infinite
            assert( ( guaranteed >= 0 || maximum < 0 ) && ( maximum < 0 || maximum > guaranteed ) );
            
            if( guaranteed < 0 ) {
                // No point in doing any cleanup if the guaranteed retention period is infinite
                continue;
            }
            
            long currentTime = clock.getCurrentTime();
            AckIdV3 forcedDeleteLevel;
            if( maximum >= 0 ) {
                forcedDeleteLevel = new AckIdV3(Math.max(0, currentTime - maximum), false );
            } else {
                forcedDeleteLevel = AckIdV3.MINIMUM;
            }
            
            AckIdV3 guaranteedRetentionLevel = new AckIdV3(Math.max(0, currentTime - guaranteed), false );
            for( StoreId storeId : manager.getStoreIdsForGroup( group ) ) {
                try {
                    boolean hasChange = false;
                    Map<String, AckIdV3> newReaderLevels = new HashMap<String, AckIdV3>();
                    AckIdV3 minReaderLevel = guaranteedRetentionLevel;
                    for( Map.Entry<String, AckIdV3> entry : manager.getReaderLevels( storeId ).entrySet() ) {
                        AckIdV3 level = entry.getValue();
                        if( level.compareTo( forcedDeleteLevel ) < 0 ) {
                            newReaderLevels.put( entry.getKey(), forcedDeleteLevel );
                            minReaderLevel = AckIdV3.min(minReaderLevel, forcedDeleteLevel);
                            hasChange = true;
                        } else {
                            newReaderLevels.put( entry.getKey(), level );
                            minReaderLevel = AckIdV3.min(minReaderLevel, level);
                        }
                    }
                    
                    if( hasChange ) {
                        manager.persistReaderLevels(storeId, newReaderLevels);
                    }
                    
                    AckIdV3 cleanupLevel = AckIdV3.min( guaranteedRetentionLevel, minReaderLevel );
                    Store store = getStore(storeId);
                    store.deleteUpTo(cleanupLevel);
                } catch( SeqStoreUnrecoverableDatabaseException e ) {
                    throw e;
                } catch( SeqStoreException e ) {
                    log.error( "Failed cleaning up " + storeId, e );
                }
            }
        }
    }
    
    public synchronized void increaseAckLevel(String storeName, String readerName, AckIdV3 ackLevel) 
            throws SeqStoreDatabaseException 
    {
        StoreId storeId = new StoreIdImpl(storeName);
        Map<String, AckIdV3> readerLevels = manager.getReaderLevels(storeId);
        
        if (readerLevels == null) {
            throw new IllegalArgumentException("Store " + storeName + " does not exist.");
        }
        
        if( !readerLevels.containsKey( readerName ) ) {
            throw new IllegalArgumentException("Store " + storeName + " does not have reader " + readerName );
        }
        
        AckIdV3 oldLevel = readerLevels.get( readerName );
        if( oldLevel.compareTo( ackLevel ) < 0 ) {
            Map<String, AckIdV3> newReaderLevels = new HashMap<String, AckIdV3>(readerLevels);
            newReaderLevels.put( readerName, ackLevel );
            manager.persistReaderLevels(storeId, newReaderLevels);
        }
    }
}
