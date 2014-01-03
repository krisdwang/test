// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.throttle.api.Throttler;

public class LocalThrottlerTest {

  Metrics metrics;

  @Before
  public void setUp() {
    this.metrics = new NullMetricsFactory().newMetrics();
  }

  static Throttler create(Map<String,String> keymap) {
    LocalThrottler.Builder b = new LocalThrottler.Builder();
    b.setMap(keymap);
    return b.build();
  }

  @Test
  public void unconfigured() {
    LocalThrottler.Builder b = new LocalThrottler.Builder();
    try {
      b.build();
      fail("must throw");
    } catch(RuntimeException e) {
    }
  }

  @Test(expected=RuntimeException.class)
  public void nullMap() {
    create(null);
  }

  @Test
  public void defaultIsUnlimited() {
    Throttler s = create(new HashMap<String,String>());
    assertFalse( s.isThrottled("whatever", metrics) );
  }

  @Test
  public void throttleA() {

    Throttler s = create(new HashMap<String,String>(){{
      put("a", "0"); // never allows a
    }});

    assertTrue( s.isThrottled("a", metrics) );
    // unknown key - should not get throttled.
    assertFalse( s.isThrottled("b", metrics) );

  }

  @Test
  public void throttleEmptyConstructor() {
    Throttler s = create(new HashMap<String, String>());
    assertFalse( s.isThrottled("", metrics) );
  }

  @Test
  public void throttleCatchAll() {
    Throttler s = create(new HashMap<String, String>(){{
      put("", "0");
    }});
    assertTrue( s.isThrottled("", metrics) );
  }

  @Test
  public void throttleAoverTime() {

    final long time[] = new long[] { 1 };
    // 2 requests, per second
    Throttler s = new LocalThrottler(new HashMap<String,String>(){{ put("a", "2"); }}) {
      @Override
      protected long getNanoTimeNow() {
        return time[0];
      }
    };

    assertFalse( s.isThrottled("a", metrics) );
    assertFalse( s.isThrottled("a", metrics) );
    assertTrue( s.isThrottled("a", metrics) );

    time[0]+=3*1000000000L; // Minimum resolution

    assertFalse( s.isThrottled("a", metrics) );
    assertFalse( s.isThrottled("a", metrics) );
    assertTrue( s.isThrottled("a", metrics) );

  }

    @Test
    public void updateRates() {
        final long time[] = new long[] { 1 };
        LocalThrottler s = new LocalThrottler(new HashMap<String, String>() {
            {
                put("a", "2");
            }
        }) {
            @Override
            protected long getNanoTimeNow() {
                return time[0];
            }
        };

        assertTrue(s.getRate("a").getLimit() == 2.0);
        s.setLimit("a", 5.0);
        assertTrue(s.getRate("a").getLimit() == 5.0);
        s.removeLimit("a");
        assertTrue(s.getRate("a") == null);
        s.setLimit("a", 3.0);
        assertTrue(s.getRate("a").getLimit() == 3.0);

        try {
            s.setLimit("a", -1.0);
            fail();
        } catch (IllegalArgumentException e) {
        }

        assertFalse(s.isThrottled("a", metrics));
        assertFalse(s.isThrottled("a", metrics));
        assertFalse(s.isThrottled("a", metrics));
        assertTrue(s.isThrottled("a", metrics));
        s.setLimit("a", 6.0);
        assertFalse(s.isThrottled("a", metrics));
        assertFalse(s.isThrottled("a", metrics));
        assertTrue(s.isThrottled("a", metrics));
    }
}
