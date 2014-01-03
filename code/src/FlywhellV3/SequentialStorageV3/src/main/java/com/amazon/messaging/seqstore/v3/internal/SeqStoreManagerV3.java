package com.amazon.messaging.seqstore.v3.internal;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.utils.Scheduler;

/**
 * This functionality has been abstracted out of SeqStoreManager to more easily
 * deal with retention management. It also matches mostly with what has been
 * done with the other portions of the SeqStore API Allows for more openness for
 * me, while keeping things properly out of the public API. Everything in here
 * should be able to remain untouched except for configuring the destinations.
 * Bonus: Simply subclass and take care of the environments and what not and
 * voila :D. BDB support rather than map support.
 */
public abstract class SeqStoreManagerV3<InfoType > implements SeqStoreManager<AckIdV3, InfoType> {

    protected final SeqStoreChildManager<InfoType> storeMan;
    
    public SeqStoreManagerV3(StorePersistenceManager persistMan,
                             AckIdSourceFactory ackIdSourceFactory, StoreSpecificClock clock,
                             ConfigProvider<InfoType> configProvider,
                             CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider,
                             Scheduler scheduler, MetricsFactory metricsFactory ) 
         throws SeqStoreDatabaseException 
    {
        this.storeMan = new SeqStoreChildManager<InfoType>(
                persistMan, ackIdSourceFactory, clock, configProvider, checkpointProvider, scheduler, metricsFactory);
    }
    
    @Override
    public boolean isHealthy() {
        return getPersistenceManager().isHealthy();
    }

    /**
     * @see com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager#createStore(com.amazon.messaging.seqstore.v3.internalInterface.StoreId)
     */
    @Override
    public SeqStoreInternalInterface<InfoType> createStore(StoreId destinationId) throws SeqStoreException {
        return storeMan.getOrCreate(destinationId);
    }

    /**
     * @see com.amazon.messaging.seqstore.v3.internal.SeqStoreManager#disableQuiescentMode()
     */
    @Override
    public final void disableQuiescentMode() throws SeqStoreException {
        for (SeqStore<?, ?> entry : storeMan.values()) {
            entry.disableQuiescentMode();
        }
    }

    /**
     * @see com.amazon.messaging.seqstore.v3.internal.SeqStoreManager#enableQuiescentMode(java.lang.String)
     */
    @Override
    public final void enableQuiescentMode(String reason) throws SeqStoreException {
        for (SeqStore<?, ?> entry : storeMan.values()) {
            entry.enableQuiescentMode(reason);
        }
    }

    /**
     * @throws SeqStoreDatabaseException 
     * @throws SeqStoreClosedException 
     * @see com.amazon.messaging.seqstore.v3.internal.SeqStoreManager#getStoreNames()
     */
    @Override
    public Set<StoreId> getStoreNames() throws SeqStoreDatabaseException, SeqStoreClosedException {
        return storeMan.getStoreIds();
    }
    
    @Override
    public boolean containsStore(StoreId store) throws SeqStoreDatabaseException, SeqStoreClosedException {
        return storeMan.containsStore(store);
    }
    
    public Set<StoreId> getStoreIdsForGroup(String group) throws SeqStoreDatabaseException, SeqStoreClosedException {
        return storeMan.getStoreIdsForGroup(group);
    }
    
    @Override
    public Set<String> getGroups() {
        return storeMan.getGroups();
    }

    /**
     * @see com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager#getStore(com.amazon.messaging.seqstore.v3.internalInterface.StoreId)
     */
    @Override
    public SeqStoreInternalInterface<InfoType> getStore(StoreId destinationId) throws SeqStoreException {
        return storeMan.get(destinationId);
    }

    @Override
    public void deleteStore(StoreId destinationId) throws SeqStoreException {
        storeMan.deleteStore(destinationId);
    }
    
    @Override
    public boolean deleteStoreIfEmpty(StoreId destinationId, long minimumEmptyTime) throws SeqStoreException {
        return storeMan.deleteStoreIfEmpty(destinationId, minimumEmptyTime);
    }
    
    @Override
    public void closeStore(StoreId store) throws SeqStoreException {
        storeMan.closeStore(store);
    }
    
    @Override
    public void prepareForClose() {
        storeMan.getPersistenceManager().prepareForClose();
    }

    /**
     * @see com.amazon.messaging.seqstore.v3.internal.SeqStoreManager#close()
     */
    @Override
    public void close() throws SeqStoreException {
        storeMan.close();
    }

    public StorePersistenceManager getPersistenceManager() {
        return storeMan.getPersistenceManager();
    }

    /**
     * Dump statistics about the store manager to stdout. This is for debugging
     * purposes only.
     * 
     * @throws SeqStoreException
     */
    @Override
    @TestOnly
    public void printDBStats() throws SeqStoreException {
        getPersistenceManager().printDBStats();
    }

    @Override
    public String toString() {
        return getPersistenceManager().toString();
    }
    
    protected Scheduler getScheduler() {
        return storeMan.getScheduler();
    }

    public void reportPerformanceMetrics(Metrics metrics) throws SeqStoreDatabaseException {
        storeMan.reportPerformanceMetrics(metrics);
    }
    
    /**
     * Wait for all in store deletions to have finished. This polls the list of stores being 
     * deleted every 20 seconds
     * @throws SeqStoreDatabaseException
     * @throws InterruptedException
     * @throws TimeoutException 
     */
    @TestOnly
    public void waitForAllStoreDeletesToFinish(long maxTime, TimeUnit unit) 
            throws SeqStoreDatabaseException, InterruptedException, TimeoutException 
    {
        storeMan.waitForAllStoreDeletesToFinish(maxTime, unit);
    }
}
