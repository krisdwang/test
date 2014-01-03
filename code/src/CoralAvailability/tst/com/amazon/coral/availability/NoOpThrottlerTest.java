// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import org.junit.Assert;
import org.junit.Test;

import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.throttle.api.Throttler;

public class NoOpThrottlerTest {
    private static final MetricsFactory metricsFactory = new NullMetricsFactory();

    private static Throttler create() {
        return new NoOpThrottler();
    }

    @Test
    public void alwaysOff() {
        Throttler t = create();
        Assert.assertFalse(t.isThrottled("tag", metricsFactory.newMetrics()));
        Assert.assertFalse(t.isThrottled("", metricsFactory.newMetrics()));
    }
}
