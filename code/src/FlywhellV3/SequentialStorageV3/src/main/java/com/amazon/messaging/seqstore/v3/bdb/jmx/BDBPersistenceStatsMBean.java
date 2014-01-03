package com.amazon.messaging.seqstore.v3.bdb.jmx;


public interface BDBPersistenceStatsMBean {
    public long getAdminBytes();
    public long getBufferBytes();
    public long getCacheTotalBytes();
    public int getCleanerBacklog();
    public long getDataBytes();
    public long getLockBytes();
    public long getNCacheMiss();
    public long getNFSyncs();
    public long getNRandomReadBytes();
    public long getNRandomReads();
    public long getNRootNodesEvicted();
    public long getNSequentialReaderBytes();
    public long getNSequentialReads();
    public long getNSequentialReadBytes();
    public long getNSequentialWriteBytes();
    public long getNSequentialWrites();
    public long getNRandomWriteBytes();
    public long getNRandomWrites();
    public long getNTempBufferWrites();
    public long getTotalLogSize();
    
    public int getNOpenBuckets();
}
