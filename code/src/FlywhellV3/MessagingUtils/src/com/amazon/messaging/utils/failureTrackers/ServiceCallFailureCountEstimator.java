package com.amazon.messaging.utils.failureTrackers;

import java.util.concurrent.TimeUnit;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.utils.RateEstimator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Cache;

/**
 * Utility class to track and retrieve the call failures per host per minute with an half life of 10 seconds.
 */
public class ServiceCallFailureCountEstimator {

    private static final double NO_RATE_DATA = 0.0;
    
    private final Cache<String, RateEstimator> failureCountEstimators;
    
    public ServiceCallFailureCountEstimator (long expirationTime, TimeUnit expirationTimeUnit) {
        this.failureCountEstimators = CacheBuilder.newBuilder().concurrencyLevel(8).expireAfterAccess(expirationTime, expirationTimeUnit).build();
    }

    public void reportFailure (final String hostName) {
        getOrCreateRateEstimator(hostName).reportEvent();
    }

    public double getFailureRatePerHost(String hostName) {
        final RateEstimator r = failureCountEstimators.getIfPresent(hostName);
        if (r != null) {
            return r.getEstimatedRate();
        }
        return NO_RATE_DATA;
    }

    private RateEstimator getOrCreateRateEstimator (final String hostName) {
        RateEstimator r = failureCountEstimators.getIfPresent(hostName);
        if (r == null) {
            failureCountEstimators.asMap().putIfAbsent(hostName, new RateEstimator());
            r = failureCountEstimators.getIfPresent(hostName);
        }
        return r;
    }
}
