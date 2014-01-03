package com.amazon.messaging.seqstore.v3.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.measure.unit.Unit;

import lombok.Data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.concurent.LockingMap;
import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreAlreadyCreatedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDeleteInProgressException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.utils.Scheduler;

import edu.umd.cs.findbugs.annotations.NonNull;

public class SeqStoreChildManager<InfoType> {
    private static final Log log = LogFactory.getLog(SeqStoreChildManager.class);
    
    // Public for testing
    public static final int MAX_STORE_DELETION_ATTEMPTS = 3;
    
    private final LockingMap<StoreId, SeqStoreInternalInterface<InfoType> > storeMap;
    
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    
    private final StorePersistenceManager persistMan;
    
    private final BucketStorageManager bucketStoreManager;
    
    private final StoreSpecificClock clock;

    private final ConfigProvider<InfoType> configProvider;
    
    private final CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider;

    private final AckIdSourceFactory ackIdSourceFactory;

    private final Scheduler scheduler;
    
    @Data
    private static class StoreDeletionRequest {
        private final StoreId storeId;
        private int attempt = 1;
    }
    
    // Used to tell the store deletion tasks to shutdown 
    private static final StoreDeletionRequest wakeupForShutdownRequest = new StoreDeletionRequest(null);
    
    private final BlockingQueue<StoreDeletionRequest> storeDeletionQueue;
    
    private final List<Thread> storeDeletionThreads;
    
    public SeqStoreChildManager(@NonNull StorePersistenceManager persistMan,
                                @NonNull AckIdSourceFactory ackIdSourceFactory, 
                                @NonNull StoreSpecificClock clock,
                                @NonNull ConfigProvider<InfoType> configProvider, 
                                CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider,
                                @NonNull Scheduler scheduler, 
                                MetricsFactory metricsFactory) throws SeqStoreDatabaseException 
    {
        if( persistMan == null ) throw new IllegalArgumentException( "persistMan cannot be null" );
        if( ackIdSourceFactory == null ) throw new IllegalArgumentException( "ackIdSourceFactory cannot be null" );
        if( clock == null ) throw new IllegalArgumentException( "clock cannot be null" );
        if( configProvider == null ) throw new IllegalArgumentException( "configProvider cannot be null" );
        if( scheduler == null ) throw new IllegalArgumentException( "scheduler cannot be null" );
        
        this.persistMan = persistMan;
        this.ackIdSourceFactory = ackIdSourceFactory;
        this.clock = clock;
        this.configProvider = configProvider;
        this.checkpointProvider = checkpointProvider;
        this.scheduler = scheduler;
        this.bucketStoreManager = new BucketStorageManager(persistMan, scheduler, metricsFactory);
        
        this.storeMap = LockingMap.builder()
                .withConcurrencyLevel(4)
                .withFairnessPolicy(false)
                .build();
        this.storeDeletionQueue = new LinkedBlockingQueue<StoreDeletionRequest>();

        int numStoreDeletionThreads = persistMan.getConfig().getNumStoreDeletionThreads();
        storeDeletionThreads = new ArrayList<Thread>( numStoreDeletionThreads );
        for( int i = 0; i < numStoreDeletionThreads; ++i ) {
            Thread thread = new Thread( new StoreDeletionTask() );
            thread.setDaemon(true);
            thread.setName("SeqStoreChildManager Store Deletion Thread-" + i);
            storeDeletionThreads.add( thread );
            thread.start();
        }
        
        for( StoreId storeId : persistMan.getStoreIdsBeingDeleted() ) {
            if( !persistMan.hasBuckets( storeId ) ) {
                persistMan.storeDeletionCompleted(storeId);
            } else {
                storeDeletionQueue.add(new StoreDeletionRequest(storeId));
            }
        }
    }
    
    private class StoreDeletionTask implements Runnable {
        @Override
        public void run() {
            while( !closed.get() ) {
                StoreDeletionRequest request = null;
                try {
                    try {
                        request = storeDeletionQueue.take();
                    } catch (InterruptedException e) {
                        // Interrupting BDB during a write can cause the BDB to be invalidated. This thread does BDB
                        // writes and so is not safe to be interrupted
                        log.error( "Interrupted in delete store thread. Interrupts can invalidate the database!", e );
                        continue;
                    }
                    
                    if( request == wakeupForShutdownRequest ) {
                        assert closed.get();
                        // Put the key back to allow other tasks to shutdown
                        try {
                            storeDeletionQueue.put(wakeupForShutdownRequest);
                        } catch (InterruptedException e) {
                            log.error( "Interrupted in delete store thread. Interrupts can invalidate the database!", e );
                        }
                        break;
                    }
                    
                    final StoreId storeId = request.getStoreId();
                    try {
                        bucketStoreManager.deleteBucketsForStore(storeId, new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    persistMan.storeDeletionCompleted(storeId);
                                } catch (SeqStoreDatabaseException e) {
                                   log.error("Failed finishing deletion for store " + storeId );
                                }
                            }
                        });
                    } catch (SeqStoreUnrecoverableDatabaseException e ) {
                        log.error( "Unrecoverable error attempting to delete " + storeId );
                        return; // The environment is bad so any further deletes would fail
                    } catch (SeqStoreDatabaseException e) {
                        log.warn( "Error deleting store " + storeId + ". Rescheduling");
                        reenqueue( request );
                    }
                } catch( Throwable e ) {
                    log.error( "Unexpected exception in bucket deletion thread", e );
                    if( request != null ) reenqueue( request );
                }
            }
        }
        
        private void reenqueue(StoreDeletionRequest request) {
            if( request.attempt < MAX_STORE_DELETION_ATTEMPTS ) {
                request.attempt++;
                for(;;) {
                    try {
                        storeDeletionQueue.put(request);
                        break;
                    } catch (InterruptedException e) {
                        log.error( "Interrupted in delete store thread. Interrupts can invalidate the database!", e );
                    }
                }
            } else {
                log.error( "Giving up on store deletion request " + request );
            }
        }
    }
    
    protected void checkOpen() throws SeqStoreClosedException {
        if( closed.get() ) {
            throw new SeqStoreClosedException("SeqStoreChildManager is closed");
        }
    }
        
    
    public SeqStoreInternalInterface<InfoType> getOrCreate(StoreId storeId) 
            throws SeqStoreException 
    {
        checkOpen();
        
        SeqStoreInternalInterface<InfoType> store = storeMap.get(storeId);
        if( store != null ) return store;
        
        store = storeMap.lock(storeId);
        try {
            if( store == null ) {
                // Recheck open - if the manager is closed after this point then the cleanup thread will
                //  block until the create completes and will handle closing the store
                checkOpen();
                store = new TopDisposableStore<InfoType>( createStore( storeId, true ) );
                storeMap.put(storeId, store);
            }
        } finally {
            storeMap.unlock(storeId);
        }
        
        return store;
    }
    
    public SeqStoreInternalInterface<InfoType> get(final StoreId storeId) throws SeqStoreException {
        checkOpen();
        
        SeqStoreInternalInterface<InfoType> result = storeMap.get(storeId);
        if (result == null && persistMan.containsStore( storeId ) ) {
            result = storeMap.lock(storeId);
            try {
                if( result == null ) {
                    // Recheck open - if the manager is closed after this point then the cleanup thread will
                    //  block until the create completes and will handle closing the store
                    checkOpen();
                    
                    SeqStoreV3<InfoType> store = createStore( storeId, false );
                    if( store == null ) {
                        // The store was opened and deleted between checking the id existed and trying to create it
                        log.info("Store " + storeId + " deleted while opening it");
                        result = null;
                    } else {
                        result = new TopDisposableStore<InfoType>(store);
                        storeMap.put(storeId, result);
                    }
                }
            } finally {
                storeMap.unlock(storeId);
            }
        }
        
        return result;
    }
    
    public Collection<SeqStoreInternalInterface<InfoType>> values() {
        return storeMap.values();
    }

    private void deleteStoreImpl(StoreId destinationId) throws SeqStoreDatabaseException {
        try {
            if( persistMan.hasBuckets( destinationId ) ) {
                persistMan.storeDeletionStarted(destinationId);
                storeDeletionQueue.add( new StoreDeletionRequest(destinationId) );
            } else {
                persistMan.deleteStoreWithNoBuckets(destinationId);
            }
        } finally {
            storeMap.remove(destinationId);
        }
    }
    
    public void deleteStore(StoreId destinationId) throws SeqStoreException {
        checkOpen();
        
        if( !containsStore(destinationId) ) {
            return;
        }
            
        SeqStoreInternalInterface<InfoType> store = storeMap.lock(destinationId);
        try {
            // If the store is already loaded close it
            if (store != null) {
                ( ( TopDisposableStore<?> ) store ).close();
            }
            
            deleteStoreImpl(destinationId);
        } finally {
            storeMap.unlock(destinationId);
        }
    }

    public boolean deleteStoreIfEmpty(StoreId destinationId, long minimumEmptyTime) throws SeqStoreException {
        checkOpen();
        
        SeqStoreInternalInterface<InfoType> store = get(destinationId);
        if( !store.isEmpty() ) return false;
        
        store = storeMap.lock(destinationId);
        try {
            if( store == null ) return false;
            
            if( !store.closeIfEmpty(minimumEmptyTime)) {
                return false;
            }
            
            deleteStoreImpl(destinationId);
            return true; 
        } finally {
            storeMap.unlock(destinationId);
        }
        
    }

    public void closeStore(StoreId destinationId) throws SeqStoreException {
        checkOpen();
        
        SeqStoreInternalInterface<InfoType> store = storeMap.lock(destinationId);
        try {
            if (store != null) {
                ( ( TopDisposableStore<?> ) store ).close();
                storeMap.remove(destinationId);
            }
        } finally {
            storeMap.unlock(destinationId);
        }
    }

    public Set<StoreId> getStoreIds() throws SeqStoreDatabaseException, SeqStoreClosedException {
        return persistMan.getStoreIds();
    }
    
    public Set<StoreId> getStoreIdsForGroup(String group) throws SeqStoreDatabaseException, SeqStoreClosedException {
        return persistMan.getStoreIdsForGroup(group);
    }
    
    public Set<String> getGroups() {
        return persistMan.getGroups();
    }
    
    /**
     * Returns true if the given store is in the manager, else false.
     */
    public boolean containsStore(StoreId store) throws SeqStoreDatabaseException, SeqStoreClosedException {
        return persistMan.containsStore(store);
    }

    public void close() throws SeqStoreException {
        if( closed.compareAndSet(false, true) ) {
            try {
                persistMan.prepareForClose();
                
                storeDeletionQueue.clear();
                storeDeletionQueue.add( wakeupForShutdownRequest );
                
                for( SeqStoreInternalInterface<InfoType> store : storeMap.values() ) {
                    try {
                        ( ( TopDisposableStore<?> ) store ).close();
                    } catch (SeqStoreException e) {
                        log.error("Failed closing " + store.getStoreName(), e);
                        if (e instanceof SeqStoreUnrecoverableDatabaseException)
                            break;
                    } catch (RuntimeException e) {
                        log.error("Internal error closing " + store.getStoreName(), e);
                    }
                }
                
                long maxWaitTime = TimeUnit.SECONDS.toNanos(5);
                long stopWaitingTime = System.nanoTime() + maxWaitTime;
                threadLoop: for( Thread thread : storeDeletionThreads ) {
                    boolean success = false;
                    do {
                        long waitTime = stopWaitingTime - System.nanoTime();
                        if( waitTime <= 0 ) {
                            log.warn( "Timedout waiting for deletion threads to shut down.");
                            break threadLoop;
                        }
                        
                        try {
                            TimeUnit.NANOSECONDS.timedJoin(thread, waitTime);
                            success = true;
                            if( thread.isAlive() ) {
                                log.warn( "Timedout waiting for deletion threads to shut down.");
                                break threadLoop;
                            }
                        } catch (InterruptedException e) {
                            log.error( "Interrupted shutting down. Interrupts can invalidate the database!", e );
                        }
                    } while( !success );
                }
                
                storeMap.clear();
                bucketStoreManager.shutdown();
                persistMan.close();
            } finally {
                closeLatch.countDown();
            }
        } else {
            boolean success = false;
            do {
                try {
                    closeLatch.await();
                    success = true;
                } catch (InterruptedException e) {
                    log.error( "Interrupted shutting down. Interrupts can invalidate the database!", e );
                }
            } while( !success );
        }
    }

    /**
     * Create a SeqStoreV3 object for the given storeId. If the store is new the initial empty ack level
     * set is persisted. This must be called with the lock for storeId held.
     * @throws SeqStoreMissingConfigException 
     * @throws SeqStoreClosedException 
     */
    private SeqStoreV3<InfoType> createStore( StoreId storeId, boolean createIfAbsent ) 
            throws SeqStoreDatabaseException, SeqStoreDeleteInProgressException, SeqStoreMissingConfigException, SeqStoreClosedException 
    {
        Map<String, AckIdV3> readerAckLevels = persistMan.getReaderLevels(storeId);
        
        
        Set<String> requiredReaders;
        try {
            requiredReaders = configProvider.getRequiredReaders(storeId.getGroupName());
        } catch (ConfigUnavailableException e) {
            throw new SeqStoreMissingConfigException( "Could not get set of required readers for " + storeId.getGroupName(), e );
        }
        
        if( requiredReaders == null ) {
            throw new SeqStoreMissingConfigException( "Could not get set of required readers for " + storeId.getGroupName() );
        }
        
        final boolean newStore;
        boolean changedAckLevels = false;
        
        if( readerAckLevels == null ) {
            if( !createIfAbsent ) return null;
            
            newStore = true;
            readerAckLevels = new HashMap<String, AckIdV3>();
            for( String reader : requiredReaders ) {
                readerAckLevels.put( reader, AckIdV3.MINIMUM );
            }
        } else {
            newStore = false;
            
            if( !readerAckLevels.keySet().containsAll( requiredReaders ) ) {
                readerAckLevels = new HashMap<String, AckIdV3>(readerAckLevels);
                
                for( String reader : requiredReaders ) {
                    if( !readerAckLevels.containsKey( reader ) ) {
                        readerAckLevels.put( reader, AckIdV3.MINIMUM );
                    }
                }
                
                changedAckLevels = true;
            }
        }
        
        SeqStoreV3<InfoType> store = new SeqStoreV3<InfoType>(
                storeId, configProvider, checkpointProvider, readerAckLevels, persistMan, bucketStoreManager, 
                ackIdSourceFactory, clock, scheduler);
        
        boolean success = false;
        try {
            if( newStore ) {
                try {
                    persistMan.createStore(storeId, readerAckLevels);
                } catch (SeqStoreAlreadyCreatedException e) {
                    // This shouldn't be possible
                    throw new IllegalStateException( "Store was created by two threads in parallel" );
                }
            } else if( changedAckLevels ) {
                persistMan.persistReaderLevels(storeId, readerAckLevels);
            }
            
            store.start();
            
            success = true;
        } finally {
            if( !success ) {
                try {
                    store.close(); 
                } catch( Throwable e ) {
                    log.error( "Error closing store that couldn't be fully opened", e );
                }
            }
        }
        
        return store;
    }

    public StorePersistenceManager getPersistenceManager() {
        return persistMan;
    }
    
    
    public Scheduler getScheduler() {
        return scheduler;
    }
    
    public void reportPerformanceMetrics(Metrics metrics) throws SeqStoreDatabaseException {
        metrics.addCount("StoreDeletionQueueSize", storeDeletionQueue.size(), Unit.ONE);
        bucketStoreManager.reportPerformanceMetrics(metrics);
        persistMan.reportPerformanceMetrics(metrics);
    }

    @TestOnly
    public void waitForAllStoreDeletesToFinish(long maxTime, TimeUnit unit) 
            throws SeqStoreDatabaseException, InterruptedException, TimeoutException 
    {
        long maxSleepTime = TimeUnit.MILLISECONDS.toNanos(20);
        long endTime = System.nanoTime() + unit.toNanos(maxTime);
        while( !storeDeletionQueue.isEmpty() ) {
            long sleepTime = Math.min( endTime - System.nanoTime(), maxSleepTime );
            if( sleepTime <= 0 ) {
                throw new TimeoutException( "Timedout waiting for store deletions to complete");
            }
            TimeUnit.NANOSECONDS.sleep( sleepTime );
        }
        Thread.sleep(150); // Give the last store deletion tasks time to run
        long remaining = endTime - System.nanoTime();
        bucketStoreManager.waitForDeletesToFinish( Math.max( remaining, 1 ), TimeUnit.NANOSECONDS);
    }
}
