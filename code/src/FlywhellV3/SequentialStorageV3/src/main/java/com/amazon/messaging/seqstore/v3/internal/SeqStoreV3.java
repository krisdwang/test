package com.amazon.messaging.seqstore.v3.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import net.jcip.annotations.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStoreMetrics;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.EnqueuesDisabledException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.jmx.SeqStoreView;
import com.amazon.messaging.seqstore.v3.internal.jmx.SeqStoreViewMBean;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.Store;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.utils.Scheduler;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Tracks the readers and the other details that should remain abstracted out.
 */
public class SeqStoreV3<InfoType> implements SeqStoreInternalInterface<InfoType> {

    private static final Log log = LogFactory.getLog(SeqStoreV3.class);
    
    private final class CleanupTask implements Runnable {

        @Override
        public void run() {
            try {
                cleaner.cleanup();
            } catch( SeqStoreUnrecoverableDatabaseException e ) {
                log.error( "Store cleanup with an unrecoverable exception. Cancelling cleanup.", e );
                cleanerPool.cancel(this, false, false);
            } catch (Exception e) {
                log.error( "Store cleanup failed", e );
            } catch( AssertionError e ) {
                log.error( "Store cleanup failed", e );
            }
            
            // Try persist the reader levels even if cleanup failed so that users won't get extra duplicates
            //  after restart.
            try {
                persistReaderLevels();
            } catch( Exception e ) {
                log.error( "Failed persisting reader levels", e );
                return;
            } catch( AssertionError e ) {
                log.error( "Failed persisting reader levels", e );
                return;
            }
            
            try {
                if( lastEnqueueTime >= lastEmptyTime ) {
                    long currentTime = clock.getCurrentTime();
                    if( messages.isEmpty() ) {
                        // Set lastEmptyTime to a timestamp from before checking if the store is empty.
                        // The other way round may result in lastEnqueueTime being before lastEmptyTime
                        // even though the store was empty.
                        lastEmptyTime = currentTime;
                    }
                }
            } catch( Exception e ) {
                log.warn( "Error updating last empty time.", e );
            } catch( AssertionError e ) {
                log.error( "Error updating last empty time.", e );
            }

            recalculateCaching();
        }
    }
    
    private final SeqStoreReaderManager<InfoType> readers;
    
    private final CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider;

    private final Store messages;

    private final StoreCleaner<InfoType> cleaner;

    private final StoreId storeId;

    private final String CLEANUP_TASK_NAME;

    private final QuiescedStateManager quiescedStateManager;

    private final CleanupTask cleaningTask;

    private final StorePersistenceManager persistenceManager;

    private final ConfigProvider<InfoType> configProvider;
    
    private final Scheduler cleanerPool;
    
    private final SeqStoreViewMBean mbean;
    
    private final StoreSpecificClock clock;
    
    @GuardedBy("self")
    private final Map<String, AckIdV3> persistedReadLevels;
    
    private final AtomicLong enqueueCount = new AtomicLong(0);
    
    private volatile SeqStoreImmutableConfig config;
    
    private final Object configUpdateLock = new Object();
    
    /// The time of the last enqueue into the store or 0 if no enqueues have been made
    private volatile long lastEnqueueTime;
    
    /// The time the store last became empty or 0 if it has never been empty since process startup
    private volatile long lastEmptyTime;
    
    private volatile boolean cacheEnqueues = true;
    
    private enum State {
        CREATED,
        STARTED,
        CLOSED
    }
    
    private volatile State state;

    public SeqStoreV3(StoreId storeName, ConfigProvider<InfoType> configProvider, 
                      CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider,
                      Map<String, AckIdV3> ackLevels,
                      StorePersistenceManager persistenceManager,
                      BucketStorageManager bucketStorageManager,
                      AckIdSourceFactory ackIdSourceFactory, StoreSpecificClock clock, 
                      Scheduler threadPool)
            throws SeqStoreDatabaseException, SeqStoreMissingConfigException, SeqStoreClosedException
    {
        this.persistenceManager = persistenceManager;
        
        String storeConfigKey = getConfigKey(storeName);
        
        try {
            this.config = configProvider.getStoreConfig(storeConfigKey);
        } catch (ConfigUnavailableException e) {
            throw new SeqStoreMissingConfigException( 
                    "Requested to create a store that does not have configuration.", e);
        }
        
        // Get the reader configurations up front to make sure they're all 
        //  available before continuing.
        Map<String, SeqStoreReaderImmutableConfig<InfoType>> readerConfigs = 
            new HashMap<String, SeqStoreReaderImmutableConfig<InfoType>>();
        
        for( String readerName : ackLevels.keySet() ) {
            try {
                readerConfigs.put(
                        readerName, configProvider.getReaderConfig(storeConfigKey, readerName) );
            } catch (ConfigUnavailableException e) {
                throw new SeqStoreMissingConfigException( 
                        "Cannot load store " + storeName + " as reader " + 
                        readerName + " is missing configuration.", e);
            }
        }
        
        this.configProvider = configProvider;
        this.checkpointProvider = checkpointProvider;
        this.storeId = storeName;
        this.quiescedStateManager = new QuiescedStateManager( storeName.toString() );
        this.cleanerPool = threadPool;
        this.persistedReadLevels = new HashMap<String, AckIdV3>();
        this.clock = clock;
        
        this.mbean = new SeqStoreView(this);

        this.CLEANUP_TASK_NAME = storeName + " Cleanup";
        this.messages = new Store(storeName, bucketStorageManager, config, ackIdSourceFactory, clock);
        if( messages.getNumBuckets() == 0 ) lastEmptyTime = clock.getCurrentTime();
        else lastEmptyTime = 0;
        
        lastEnqueueTime = 0;

        this.readers = new SeqStoreReaderManager<InfoType>(this, messages, ackLevels, readerConfigs);

        this.cleaner = new StoreCleaner<InfoType>(messages, readers, config);

        this.cleaningTask = new CleanupTask();
        state = State.CREATED;
    }
    
    public void start() {
        if( state != State.CREATED ) {
            throw new IllegalStateException("Start called while store was in state " + state );
        }
        state = State.STARTED;
        cleanerPool.executePeriodically(CLEANUP_TASK_NAME, cleaningTask, config.getCleanerPeriod(), false);
    }

    public void close() throws SeqStoreDatabaseException {
        if( state == State.CLOSED ) {
            return;
        }
        
        State initialState = state;
        state = State.CLOSED;
        if( initialState == State.STARTED ) {
            // close all the readers.
            cleanerPool.cancel(cleaningTask, false, true);
            persistReaderLevels();
        }
        
        readers.close();
        messages.close();
    }
    
    @Override
    public boolean isOpen() {
        return state == State.STARTED;
    }
    
    @Override
    public SeqStoreReaderV3InternalInterface<InfoType> createReader(String readerName) throws SeqStoreException {
        SeqStoreReaderV3InternalInterface<InfoType> disposable = readers.getOrCreate(readerName);
        persistReaderLevels();
        return disposable;
    }

    private void persistReaderLevels() throws SeqStoreDatabaseException {
        synchronized (persistedReadLevels) {
            Map<String, AckIdV3> readerLevels = new HashMap<String, AckIdV3>(readers.keySet().size());
            for (SeqStoreReaderV3InternalInterface<InfoType> reader : readers.values()) {
                try {
                    readerLevels.put(reader.getReaderName(), reader.getAckLevel());
                } catch( SeqStoreClosedException e ) {
                    // Threads the delete or close readers must always flush the reader levels 
                    log.info( 
                            "Reader deleted or closed while calculating ack levels. Leaving read level persistence to " +
                            "whatever thread deleted or closed the reader.", e );
                    return;
                }
            }
            
            boolean shouldPersistReadLevels = !readerLevels.equals( persistedReadLevels );
            
            if( shouldPersistReadLevels ) {
                persistenceManager.persistReaderLevels(storeId, readerLevels);
                persistedReadLevels.clear();
                persistedReadLevels.putAll( readerLevels );
            }
        }
    }
    
    @Override
    public void enableQuiescentMode(String reason) {
        quiescedStateManager.enableQuiescentMode( reason );
    }

    @Override
    public void disableQuiescentMode() {
        quiescedStateManager.disableQuiescentMode();
    }

    @Override
    public void enqueue(Entry message, long timeout, Metrics metrics) throws SeqStoreException {
        AckIdV3 ackId = getAckIdForEnqueue(message.getAvailableTime());
        try {
            enqueue( ackId, message, timeout, cacheEnqueues, metrics );
        } finally {
            enqueueFinished(ackId);
        }
    }
    
    @Override
    public AckIdV3 getAckIdForEnqueue(long requestedTime) throws EnqueuesDisabledException {
        quiescedStateManager.throwIfQuiesced();
        lastEnqueueTime = clock.getCurrentTime();
        return messages.getAckIdForEnqueue(requestedTime);
    }
    
    @Override
    public void enqueueFinished(AckIdV3 ackId) {
        messages.enqueueFinished(ackId);
        for (SeqStoreReaderV3InternalInterface<InfoType> entry : readers.values()) {
            try {
                entry.messageEnqueued(ackId.getTime());
            } catch (SeqStoreClosedException e) {
                log.info( entry.getReaderName() + " removed or closed while in notifyReaders");
            }
        }
    }
    
    @Override
    public void enqueue(AckIdV3 ackId, Entry message, long timeout, boolean cache, Metrics metrics) 
        throws SeqStoreException 
    {
        quiescedStateManager.throwIfQuiesced();
        lastEnqueueTime = clock.getCurrentTime(); // Can't do this only in getAckIdForEnqueue as that isn't always called
        messages.enqueue(ackId, message, cache && cacheEnqueues, metrics);
        enqueueCount.incrementAndGet();
    }
    
    @Override
    public AckIdV3 getMinEnqueueLevel() {
        return messages.getMinEnqueueLevel();
    }
    
    @Override
    public AckIdSource getAckIdSource() throws SeqStoreClosedException {
        return messages.getAckIdSource();
    }
    
    @Override
    @TestOnly
    public String getStoreName() {
        return storeId.getStoreName();
    }
    
    static String getConfigKey(StoreId storeId) {
        return storeId.getGroupName();
    }
    
    /**
     * @return the config key used to get the store config from the config provider.
     */
    @Override
    public String getConfigKey() {
        return getConfigKey(storeId);
    }

    @Override
    public StoreId getStoreId() {
        return storeId;
    }

    @Override
    public long getEnqueueCount() {
        return enqueueCount.get();
    }

    @Override
    public SeqStoreReaderV3InternalInterface<InfoType> getReader(String readerId) throws SeqStoreClosedException {
        return readers.get(readerId);
    }

    @Override
    public Set<String> getReaderNames() {
        return readers.keySet();
    }

    @Override
    public void removeReader(String readerId) throws SeqStoreException {
        if (readers.removeReader(readerId) ) {
            persistReaderLevels();
        }
    }

    /**
     * Note: This function must be externally synchronized to prevent parallel enqueues or 
     * it may close the store just after a successful enqueue
     */
    @Override
    public boolean closeIfEmpty(long minimumEmptyTime) throws SeqStoreException {
        long tmpLastEmptyTime = lastEmptyTime;
        
        if( tmpLastEmptyTime > 0 && tmpLastEmptyTime > lastEnqueueTime && 
            ( clock.getCurrentTime() - tmpLastEmptyTime ) >= minimumEmptyTime && 
            messages.isEmpty() ) 
        {
            close();
            return true;
        }
        
        return false;
    }

    @Override
    public void updateConfig(SeqStoreImmutableConfig newConfig, Set<String> requiredReaders) throws SeqStoreException {
        synchronized (configUpdateLock) {
            if( !newConfig.equals( this.config ) ) {
                this.config = newConfig;
                cleaner.setConfig(newConfig);
                if( !cleanerPool.cancel(cleaningTask, false, false) )
                    throw new IllegalStateException("Rescheduling of cleaner task failed.");
                cleanerPool.executePeriodically(CLEANUP_TASK_NAME, cleaningTask, newConfig.getCleanerPeriod(), false);
            }
            
            for( String readerName : requiredReaders ) {
                if( !readers.keySet().contains( readerName ) ) {
                    createReader(readerName);
                }
            }
        }
    }

    @Override
    public void updateConfig() throws SeqStoreException {
        synchronized (configUpdateLock) {
            try {
                updateConfig( 
                        configProvider.getStoreConfig(getConfigKey()), 
                        configProvider.getRequiredReaders( getConfigKey()) );
            } catch (ConfigUnavailableException e) {
                throw new SeqStoreMissingConfigException( 
                        "Unable to find new updated configuration for store " + getConfigKey(), e );
            }
        }
    }

    @Override
    public void runCleanupNow() {
        cleaningTask.run();
    }

    @Override
    public AckIdV3 getMinAvailableLevel() throws SeqStoreClosedException {
        return messages.getMinAvailableLevel();
    }
    
    @Override
    public SeqStoreMetrics getStoreMetrics() throws SeqStoreException {
        return messages.getStoreMetrics();
    }
    
    @Override
    public SeqStoreMetrics getActiveStoreMetrics() throws SeqStoreException {
        return getStoreMetrics();
    }
    
    @Override
    public long getNumBuckets() {
        return messages.getNumBuckets();
    }
    
    @Override
    public long getNumDedicatedBuckets() {
        return messages.getNumDedicatedBuckets();
    }
    
    @Override
    public Object getMBean() {
        return mbean;
    }

    @Override
    public boolean isEmpty() throws SeqStoreClosedException, SeqStoreDatabaseException {
        return messages.isEmpty();
    }
    
    public ConfigProvider<InfoType> getConfigProvider() {
        return configProvider;
    }
    
    public CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> getCheckpointProvider() {
        return checkpointProvider;
    }
    
    public StoreSpecificClock getClock() {
        return clock;
    }
    
    @Override
    public SeqStoreImmutableConfig getConfig() {
        return config;
    }

    private void recalculateCaching() {
        long maxEstimatedCacheTimeMillis =
                TimeUnit.SECONDS.toMillis( config.getMaxEstimatedCacheTimeSeconds() );
        
        NavigableMap<AckIdV3, List<SeqStoreReaderV3InternalInterface<?>>> readersByPosition 
            = Maps.newTreeMap();
        for( SeqStoreReaderV3InternalInterface<?> reader : readers.values() ) {
            try {
                AckIdV3  readLevel = reader.getReadLevel();
                List<SeqStoreReaderV3InternalInterface<?>> readersAtLevel = readersByPosition.get(readLevel);
                if( readersAtLevel == null ) {
                    readersAtLevel = Lists.newArrayList();
                    readersByPosition.put(readLevel, readersAtLevel);
                }
                readersAtLevel.add(reader);
            } catch (SeqStoreClosedException e) {
                log.info( "Reader " + reader.getReaderName() + " closed while cleanup was running", e);
            }
        }
        
        Map.Entry<AckIdV3, List<SeqStoreReaderV3InternalInterface<?>>> previousEntry = null;
        boolean previousEvictFromCache = true;
        
        for( Map.Entry<AckIdV3, List<SeqStoreReaderV3InternalInterface<?>>> entry : readersByPosition.entrySet() ) {
            boolean evictFromCache = true;
            if( entry.getValue().size() > 1 ) {
                // Multiple readers all reading from the same entries
                evictFromCache = false;
            } 
            
            if( previousEntry != null ) {
                long timeToPreviousInMS = 
                        entry.getKey().getTime() - previousEntry.getKey().getTime();
                
                // If the previous reader is less than maxEstimatedCacheTimeMillis behind
                // then keep entries in the cache for it
                if( timeToPreviousInMS < maxEstimatedCacheTimeMillis) 
                {
                    evictFromCache = false;
                }
                
                // If the previous reader is less than 2 times the cleaner
                // period behind then it should cache as it may change places
                // with this one before the next cleaner pass
                // then it should cache as its possible they'll change places
                if( timeToPreviousInMS < config.getCleanerPeriod() * 2 ) {
                    previousEvictFromCache = false;
                }
                
                for( SeqStoreReaderV3InternalInterface<?> reader : previousEntry.getValue() ) {
                    try {
                        reader.setEvictFromCacheAfterDequeueFromStore(previousEvictFromCache);
                    } catch (SeqStoreClosedException e) {
                        log.info( "Reader " + reader.getReaderName() + " closed while cleanup was running", e);
                    }
                }
            }
            
            previousEvictFromCache = evictFromCache;
            previousEntry = entry;
        }
        
        if( previousEntry != null ) {
            for( SeqStoreReaderV3InternalInterface<?> reader : previousEntry.getValue() ) {
                try {
                    reader.setEvictFromCacheAfterDequeueFromStore(previousEvictFromCache);
                } catch (SeqStoreClosedException e) {
                    log.info( "Reader " + reader.getReaderName() + " closed while cleanup was running", e);
                }
            }
            
            // If the closest reader is less than maxEstimatedCacheTimeMillis behind
            // then keep entries in the cache for it
            long timeTillMinEnqueueLevel = 
                    messages.getMinEnqueueLevel().getTime() - previousEntry.getKey().getTime();
            if( timeTillMinEnqueueLevel < maxEstimatedCacheTimeMillis ) {
                cacheEnqueues = true;
            } else {
                cacheEnqueues = false;
            }
        } else {
            cacheEnqueues = false;
        }
    }
    
    @Override
    @TestOnly
    public boolean isCacheEnqueues() {
        return cacheEnqueues;
    }
}
