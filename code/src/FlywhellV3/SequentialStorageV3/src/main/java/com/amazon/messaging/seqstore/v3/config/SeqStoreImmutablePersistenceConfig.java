package com.amazon.messaging.seqstore.v3.config;

import java.io.File;

import edu.umd.cs.findbugs.annotations.CheckForNull;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import net.jcip.annotations.Immutable;

@Data @EqualsAndHashCode(doNotUseGetters=true)
@ToString(includeFieldNames=true)
@Immutable
public class SeqStoreImmutablePersistenceConfig implements SeqStorePersistenceConfigInterface {
    
    private final boolean openReadOnly;
    private final boolean truncateDatabase;
    private final File storeDirectory;
    @CheckForNull
    private final File mainJePropertiesFile;
    @CheckForNull
    private final File longLivedMessagesJePropertiesFile;
    private final long dirtyMetadataFlushPeriod;
    private final int numDedicatedDBDeletionThreads;
    private final int numSharedDBDeletionThreads;
    private final int numStoreDeletionThreads;
    
    @Getter(AccessLevel.NONE)
    private final boolean useSeperateEnvironmentForDedicatedBuckets;
    
    @Override
    public boolean useSeperateEnvironmentForDedicatedBuckets() {
        return useSeperateEnvironmentForDedicatedBuckets;
    }
}
