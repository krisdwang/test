package com.amazon.messaging.seqstore.v3.config;

public abstract class DurabilityPolicy {

    public abstract int getDesiredRedundancy();

    public abstract int getRequiredRedundancy();

    public abstract DistributionPolicy getDistributionPolicy();
}
