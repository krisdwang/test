// vim: et ts=8 sts=4 sw=4 tw=0
package com.amazon.coral.avail.throttlers;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.throttle.api.Throttler;

/**
 * Throttler that never throttles.
 */
public class NoOpThrottler extends Throttler {
    @Override
    public boolean isThrottled(CharSequence key, Metrics metrics) {
        return false;
    }
}
