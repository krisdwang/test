// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import junit.framework.Assert;

import org.junit.Test;

import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.NullMetricsFactory;

public class ThrottlerAdapterTest {
    private static final MetricsFactory metricsFactory = new NullMetricsFactory();

    @Test(expected = RuntimeException.class)
    public void nullArgument() {
        new ThrottlerAdapter(null);
    }

    @Test
    public void callIsThrottled() {
        final CharSequence tag = "TestTag";
        final Boolean[] isCalled = new Boolean[1];
        ThrottlerAdapter adapter = new ThrottlerAdapter(new com.amazon.coral.availability.ThrottlingStrategy() {

            @Override
            public boolean isThrottled(CharSequence key) {
                Assert.assertEquals(tag, key);
                isCalled[0] = Boolean.TRUE;
                return false;
            }

        });

        adapter.isThrottled(tag, metricsFactory.newMetrics());
        Assert.assertTrue(isCalled[0]);
    }
}
