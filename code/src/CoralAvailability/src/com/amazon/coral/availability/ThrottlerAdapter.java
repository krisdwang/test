// vim: et ts=8 sts=4 sw=4 tw=0
package com.amazon.coral.availability;

import com.amazon.coral.google.common.base.Preconditions;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.availability.ThrottlingStrategy;

/**
 * This adapter presents a {@code com.amazon.coral.availability.ThrottlingStrategy} as a {@code Throttler}
 * @deprecated Prefer com.amazon.coral.throttle.api.Throttler
 */
@Deprecated
public final class ThrottlerAdapter extends Throttler {
    private final ThrottlingStrategy strategy;

    public ThrottlerAdapter(ThrottlingStrategy strategy) {
        Preconditions.checkNotNull(strategy, "ThrottlerAdatper(null strategy)");
        this.strategy = strategy;
    }

    @Override
    public boolean isThrottled(CharSequence key, Metrics metrics) {
        return strategy.isThrottled(key);
    }
}
