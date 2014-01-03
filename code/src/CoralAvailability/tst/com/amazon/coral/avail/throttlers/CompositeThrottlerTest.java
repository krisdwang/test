// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;

public class CompositeThrottlerTest {

  @Test(expected=IllegalArgumentException.class)
  public void nullStrategies() {
    Throttler[] s = null;
    new CompositeThrottler(s);
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullStrategy() {
    new CompositeThrottler(new Throttler[] { null });
  }

  @Test
  public void delegates() {
    final int count[] = new int[]{0};
    Throttler s = new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics m) {
        count[0]++;
        return true;
      }
    };
    Metrics m = new NullMetricsFactory().newMetrics();
    s = new CompositeThrottler(s);
    s.isThrottled("a", m);
    assertEquals(1, count[0]);
  }

}
