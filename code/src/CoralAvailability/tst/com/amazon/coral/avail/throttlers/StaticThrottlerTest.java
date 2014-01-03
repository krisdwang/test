// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import java.util.List;
import java.util.LinkedList;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.throttle.api.Throttler;

import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.Mockito;

public class StaticThrottlerTest {

  @Test(expected=IllegalArgumentException.class)
  public void nullBlacklist() {
    StaticThrottler.newBlacklist(null);
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullWhitelist() {
    StaticThrottler.newWhitelist(null);
  }

  @Test
  public void blacklist() {
    List<CharSequence> keys = new LinkedList<CharSequence>();
    keys.add("foo");
    keys.add("bar");
    Metrics m = Mockito.mock(Metrics.class);
    Throttler strategy = StaticThrottler.newBlacklist(keys);
    assertTrue(strategy.isThrottled("foo", m));
    assertTrue(strategy.isThrottled("bar", m));

    StringBuilder sb = new StringBuilder();
    sb.append("fo");
    sb.append("o");
    assertTrue(strategy.isThrottled(sb, m));

    assertFalse(strategy.isThrottled("foobar", m));
    assertFalse(strategy.isThrottled("", m));
  }

  @Test
  public void whitelist() {
    List<CharSequence> keys = new LinkedList<CharSequence>();
    keys.add("foo");
    keys.add("bar");
    Metrics m = Mockito.mock(Metrics.class);
    Throttler strategy = StaticThrottler.newWhitelist(keys);
    assertFalse(strategy.isThrottled("foo", m));
    assertFalse(strategy.isThrottled("bar", m));
    assertTrue(strategy.isThrottled("foobar", m));
    assertTrue(strategy.isThrottled("", m));
  }

}
