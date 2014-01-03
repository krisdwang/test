// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.measure.unit.Unit;

import org.junit.Test;

import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.throttle.api.Throttler;

public class MemoizedThrottlerTest {

  @Test(expected=RuntimeException.class)
  public void nullThrottler() {
    new MemoizedThrottler(null);
  }

  @Test
  public void memoizedThrottler() {

    final int count[] = new int[]{0};

    Throttler s = new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics metrics) {
        count[0]++;
        return true;
      }
    };
    s = new MemoizedThrottler(s);

    assertTrue( s.isThrottled("a", null) );
    assertTrue( s.isThrottled("a", null) );
    assertTrue( s.isThrottled("a", null) );
    assertEquals(count[0], 1);
  }

  @Test
  public void memoizedMetrics() {

    final Metrics[] r = new Metrics[1];

    Throttler t = new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics metrics) {
        r[0] = metrics;
        return false;
      }
    };
    t = new MemoizedThrottler(t);

    Metrics m = new NullMetricsFactory().newMetrics();

    t.isThrottled("foo", m);
    assertEquals(m, r[0]);
  }

}
