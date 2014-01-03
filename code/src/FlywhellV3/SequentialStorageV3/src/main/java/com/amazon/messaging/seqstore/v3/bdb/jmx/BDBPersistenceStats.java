package com.amazon.messaging.seqstore.v3.bdb.jmx;

import com.amazon.messaging.seqstore.v3.bdb.BDBPersistentManager;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.sleepycat.je.EnvironmentStats;


public class BDBPersistenceStats implements BDBPersistenceStatsMBean {
    private static final long maxStatAge = 5000; // 5 seconds
    
    private final BDBPersistentManager persistenceManager;
    
    private volatile EnvironmentStats cachedMainStats;
    private volatile EnvironmentStats cachedLLMStats;
    private volatile long cacheTime = -1;
    
    public BDBPersistenceStats(BDBPersistentManager persistenceManager) {
        this.persistenceManager = persistenceManager;
    }

    private void updateCacheIfNecessary() {
        if( System.currentTimeMillis() - cacheTime > maxStatAge ) {
            try {
                cachedLLMStats= persistenceManager.getLongLivedMessagesEnvironmentStats();
                cachedMainStats = persistenceManager.getMainEnvironmentStats();
            } catch (SeqStoreException e) {
                throw new RuntimeException("Failed reading DB stats", e );
            }
        }
    }
    
    @Override
    public long getAdminBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getAdminBytes() + cachedLLMStats.getAdminBytes();
    }

    @Override
    public long getBufferBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getBufferBytes() + cachedLLMStats.getBufferBytes();
    }

    @Override
    public long getCacheTotalBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getCacheTotalBytes() + cachedLLMStats.getCacheTotalBytes();
    }

    @Override
    public long getDataBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getDataBytes() + cachedLLMStats.getDataBytes();
    }

    @Override
    public long getLockBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getLockBytes() + cachedLLMStats.getLockBytes();
    }

    @Override
    public long getNRandomReadBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getNRandomReadBytes() + cachedLLMStats.getNRandomReadBytes();
    }

    @Override
    public long getNRandomReads() {
        updateCacheIfNecessary();
        return cachedMainStats.getNRandomReads() + cachedLLMStats.getNRandomReads();
    }

    @Override
    public long getNRandomWriteBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getNRandomWriteBytes() + cachedLLMStats.getNRandomWriteBytes();
    }

    @Override
    public long getNRandomWrites() {
        updateCacheIfNecessary();
        return cachedMainStats.getNRandomWrites() + cachedLLMStats.getNRandomWrites();
    }

    @Override
    public long getNRootNodesEvicted() {
        updateCacheIfNecessary();
        return cachedMainStats.getNRootNodesEvicted() + cachedLLMStats.getNRootNodesEvicted();
    }

    @Override
    public long getNSequentialReadBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getNSequentialReadBytes() + cachedLLMStats.getNSequentialReadBytes();
    }

    @Override
    public long getNSequentialReads() {
        updateCacheIfNecessary();
        return cachedMainStats.getNSequentialReads() + cachedLLMStats.getNSequentialReads();
    }

    @Override
    public long getNSequentialWriteBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getNSequentialWriteBytes() + cachedLLMStats.getNSequentialWriteBytes();
    }

    @Override
    public long getNSequentialWrites() {
        updateCacheIfNecessary();
        return cachedMainStats.getNSequentialWrites() + cachedLLMStats.getNSequentialWrites();
    }

    @Override
    public long getNTempBufferWrites() {
        updateCacheIfNecessary();
        return cachedMainStats.getNTempBufferWrites() + cachedLLMStats.getNTempBufferWrites();
    }

    @Override
    public int getCleanerBacklog() {
        updateCacheIfNecessary();
        return cachedMainStats.getCleanerBacklog() + cachedLLMStats.getCleanerBacklog();
    }

    @Override
    public long getNCacheMiss() {
        updateCacheIfNecessary();
        return cachedMainStats.getNCacheMiss() + cachedLLMStats.getNCacheMiss();
    }

    @Override
    public long getNFSyncs() {
        updateCacheIfNecessary();
        return cachedMainStats.getNFSyncs() + cachedLLMStats.getNFSyncs();
    }

    @Override
    public long getNSequentialReaderBytes() {
        updateCacheIfNecessary();
        return cachedMainStats.getNSequentialReadBytes() + cachedLLMStats.getNSequentialReadBytes();
    }

    @Override
    public long getTotalLogSize() {
        updateCacheIfNecessary();
        return cachedMainStats.getTotalLogSize() + cachedLLMStats.getTotalLogSize();
    }

    @Override
    public int getNOpenBuckets() {
        updateCacheIfNecessary();
        return persistenceManager.getNumOpenBuckets();
    }

}
