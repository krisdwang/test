// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import org.junit.Test;

import com.amazon.coral.availability.DThrottleSharedClient;
import com.amazon.coral.metrics.*;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Set;
import java.util.HashSet;

public class DistributedThrottlerTest {

  @Test
  public void uriTestNoScheme() throws Throwable {
    URI u = DistributedThrottler.getURI("localhost:6969");

    assertEquals("localhost", u.getHost());
    assertEquals(6969, u.getPort());
  }

  @Test
  public void uriTestScheme() throws Throwable {
    URI u = DistributedThrottler.getURI("tcp://localhost:6969");

    assertEquals("localhost", u.getHost());
    assertEquals(6969, u.getPort());
  }

  @Test
  public void checkKeys() throws Throwable {
    final Set<String> actual = new HashSet<String>();

    DThrottleSharedClient client = new DThrottleSharedClient("localhost", 6969) {
      @Override
      public boolean query(CharSequence key) {
        actual.add(key.toString());
        return false;
      }
    };

    Metrics metrics = new NullMetricsFactory().newMetrics();
    DistributedThrottler throttler = new DistributedThrottler(client);
    throttler.isThrottled("foo", metrics);
    throttler.isThrottled("bar", metrics);
    throttler.isThrottled("", metrics);

    Set<String> expected = new HashSet<String>();
    expected.add("foo");
    expected.add("bar");

    assertEquals(expected, actual);
  }
}
