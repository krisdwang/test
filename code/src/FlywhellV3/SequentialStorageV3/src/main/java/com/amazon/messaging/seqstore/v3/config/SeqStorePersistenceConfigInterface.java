package com.amazon.messaging.seqstore.v3.config;

import java.io.File;

import edu.umd.cs.findbugs.annotations.CheckForNull;

public interface SeqStorePersistenceConfigInterface {
    public boolean isOpenReadOnly();
    public File getStoreDirectory();
    public boolean isTruncateDatabase();
    
    @CheckForNull
    public File getMainJePropertiesFile();
    
    @CheckForNull
    public File getLongLivedMessagesJePropertiesFile();
    
    public long getDirtyMetadataFlushPeriod();
    
    public boolean useSeperateEnvironmentForDedicatedBuckets();
   
    public int getNumDedicatedDBDeletionThreads();
    
    public int getNumSharedDBDeletionThreads();
    
    public int getNumStoreDeletionThreads();
}
