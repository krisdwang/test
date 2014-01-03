package com.amazon.messaging.seqstore.v3.config;

import net.jcip.annotations.Immutable;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(includeFieldNames=true)
@Immutable
public class SeqStoreImmutableConfig implements SeqStoreConfigInterface {        
    private final int desiredRedundancy;
    private final int requiredRedundancy;
    private final DistributionPolicy distributionPolicy;
    private final long maxMessageLifeTime;
    private final long guaranteedRetentionPeriod;
    private final long cleanerPeriod;
    private final BucketStoreConfig dedicatedDBBucketStoreConfig;
    private final BucketStoreConfig sharedDBBucketStoreConfig;
    private final int maxSizeInKBBeforeUsingDedicatedDBBuckets;
    private final int minBucketsToCompress;
    private final int maxEstimatedCacheTimeSeconds;
}