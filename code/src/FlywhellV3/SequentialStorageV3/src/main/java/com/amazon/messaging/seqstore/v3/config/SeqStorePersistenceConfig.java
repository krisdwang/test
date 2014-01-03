package com.amazon.messaging.seqstore.v3.config;

import java.io.File;

import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;

import edu.umd.cs.findbugs.annotations.CheckForNull;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Data @ToString(includeFieldNames=true) @EqualsAndHashCode(doNotUseGetters=true)
public class SeqStorePersistenceConfig implements SeqStorePersistenceConfigInterface {

    /**
     * Location of BDB files.
     */
    private File storeDirectory;

    /**
     * When true all content of database is erased each time manager is
     * instantiated. To be used for unit tests. Default is <code>false</code>
     */
    private boolean truncateDatabase = false;

    private boolean openReadOnly = false;

    /**
     * Option pointer to a je.properties file to be used with the BDB environment
     */
    @CheckForNull
    private File mainJePropertiesFile;
    
    /**
     * Option pointer to a je.properties file to be used with the BDB environment for long lived messages
     */
    @CheckForNull
    private File longLivedMessagesJePropertiesFile;

    /**
     * Flush bucket metadata every 30 seconds
     */
    private long dirtyMetadataFlushPeriod = 30 * 1000;
    
    private int numDedicatedDBDeletionThreads = 1;
    
    private int numSharedDBDeletionThreads = 3;
    
    private int numStoreDeletionThreads = 2;
    
    @Getter(AccessLevel.NONE) @Setter(AccessLevel.NONE)
    private boolean useSeperateEnvironmentForDedicatedBuckets = true;
    
    public SeqStorePersistenceConfig() {
    }
    
    public SeqStorePersistenceConfig(SeqStorePersistenceConfigInterface config) {
        this.storeDirectory = config.getStoreDirectory();
        this.truncateDatabase = config.isTruncateDatabase();
        this.openReadOnly = config.isOpenReadOnly();
        this.mainJePropertiesFile = config.getMainJePropertiesFile();
        this.longLivedMessagesJePropertiesFile = config.getLongLivedMessagesJePropertiesFile();
        this.dirtyMetadataFlushPeriod = config.getDirtyMetadataFlushPeriod();
        this.useSeperateEnvironmentForDedicatedBuckets = config.useSeperateEnvironmentForDedicatedBuckets();
    }

    /**
     * Constructs an immutable config object from this object after validating it.
     * 
     * @return An immutable copy of this config
     * @throws InvalidConfigException if the config is not valid according to {@link #validate()}
     */
    public SeqStoreImmutablePersistenceConfig getImmutableConfig() throws InvalidConfigException {
        String validResult = validate();
        if( validResult != null ) throw new InvalidConfigException( validResult );
        return new SeqStoreImmutablePersistenceConfig(
                openReadOnly,truncateDatabase, storeDirectory, 
                mainJePropertiesFile, longLivedMessagesJePropertiesFile,
                dirtyMetadataFlushPeriod, numDedicatedDBDeletionThreads, numSharedDBDeletionThreads, numStoreDeletionThreads,
                useSeperateEnvironmentForDedicatedBuckets );   
    }

    /**
     * Return if a separate environment will be used for dedicated buckets.
     */
    public boolean useSeperateEnvironmentForDedicatedBuckets() {
        return useSeperateEnvironmentForDedicatedBuckets;
    }
    
    /**
     * Set if a separate environment will be used for dedicated buckets. Defaults to true. If this is true
     * all new dedicated buckets will created in a separate environment. This can make cleanup better
     * by keeping long lived messages separate from short lived messages at the cost of extra disk seeks at write.
     * <p>
     * If this is false the long lived messages environment will still be checked for any messages that may
     * have been stored earlier. 
     */
    public void setUseSeperateEnvironmentForDedicatedBuckets(boolean useSeperateEnvironmentForDedicatedBuckets) {
        this.useSeperateEnvironmentForDedicatedBuckets = useSeperateEnvironmentForDedicatedBuckets;
    }

    private String validate() {
        if (storeDirectory == null ) 
            return "Store directory must be set.";
        if( truncateDatabase && openReadOnly ) 
            return "Only one of truncate database and openReadOnly may be set";
        return null;
    }
    
    /**
     * Set how often to flush bucket metadata to disk. After a restart the bucket entry and byte counts
     * can be off by up to this amount of time.
     * 
     * @param time period in milliseconds between flushes
     */
    public void setDirtyMetadataFlushPeriod(long time) {
        dirtyMetadataFlushPeriod = time;
    }
    
    /**
     * Set the number of threads dedicated to deleting dedicated database
     * buckets. Defaults to 1.
     * 
     * @param numDedicatedDBDeletionThreads
     */
    public void setNumDedicatedDBDeletionThreads(int numDedicatedDBDeletionThreads) {
        this.numDedicatedDBDeletionThreads = numDedicatedDBDeletionThreads;
    }
    
    /**
     * Set the number of threads dedicated to deleting shared database
     * buckets. Defaults to 3.
     * 
     * @param numDedicatedDBDeletionThreads
     */
    public void setNumSharedDBDeletionThreads(int numSharedDBDeletionThreads) {
        this.numSharedDBDeletionThreads = numSharedDBDeletionThreads;
    }
    
    /**
     * Set the number of threads dedicated to deleting stores. Note that these threads
     * just mark all the buckets in the store as being eligible for deletion. Deletion
     * of the buckets for the store happens in the appropriate bucket deletion thread 
     * 
     * Defaults to 2. 
     * 
     * @param numDedicatedDBDeletionThreads
     */
    public void setNumStoreDeletionThreads(int numStoreDeletionThreads) {
        this.numStoreDeletionThreads = numStoreDeletionThreads;
    }
}
