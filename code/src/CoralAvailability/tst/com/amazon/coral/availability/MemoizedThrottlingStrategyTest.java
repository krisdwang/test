package com.amazon.coral.availability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.measure.unit.Unit;

import org.junit.Test;

import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.throttle.api.Throttler;

public class MemoizedThrottlingStrategyTest {

  @Test(expected=IllegalArgumentException.class)
  public void nullStrategy() {
    new MemoizedThrottlingStrategy((ThrottlingStrategy)null);
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullThrottler() {
    new MemoizedThrottlingStrategy((Throttler)null);
  }

  @Test
  public void memoizedStrategy() {

    final int count[] = new int[]{0};
    final CharSequence keys[] = new String[2];

    ThrottlingStrategy s = new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        keys[count[0]] = key;
        count[0]++;

        return true;
      }
    };
    s = new MemoizedThrottlingStrategy(s);

    assertTrue( s.isThrottled("a") );
    assertTrue( s.isThrottled("a") );
    assertTrue( s.isThrottled("a") );
    assertEquals(count[0], 1);

    ((Throttler)s).isThrottled(ThrottlingHandler.DEFAULT_KEY, new NullMetricsFactory().newMetrics());
    ((Throttler)s).isThrottled(ThrottlingHandler.DEFAULT_KEY, new NullMetricsFactory().newMetrics());

    assertEquals(count[0], 2); // one for "a", one for DEFAULT_KEY
    assertEquals("a", keys[0]);
    assertEquals(ThrottlingHandler.DEFAULT_KEY, keys[1]);
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
    s = new MemoizedThrottlingStrategy(s);

    assertTrue( s.isThrottled("a", null) );
    assertTrue( s.isThrottled("a", null) );
    assertTrue( s.isThrottled("a", null) );
    assertEquals(count[0], 1);

    assertTrue( s.isThrottled("", null) );
    assertTrue( s.isThrottled("", null) );
    assertTrue( s.isThrottled("", null) );
    assertEquals(count[0], 2);
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
    t = new MemoizedThrottlingStrategy(t);

    Metrics m = new NullMetricsFactory().newMetrics();

    t.isThrottled("foo", m);
    assertEquals(m, r[0]);
  }

}
