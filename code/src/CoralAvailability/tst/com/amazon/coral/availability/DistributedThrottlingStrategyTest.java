package com.amazon.coral.availability;

import org.junit.Test;

import com.amazon.coral.availability.DThrottleSharedClient;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Set;
import java.util.HashSet;

public class DistributedThrottlingStrategyTest {

  @Test
  public void uriTestNoScheme() throws Throwable {
    URI u = DistributedThrottlingStrategy.getURI("localhost:6969");

    assertEquals("localhost", u.getHost());
    assertEquals(6969, u.getPort());
  }

  @Test
  public void uriTestScheme() throws Throwable {
    URI u = DistributedThrottlingStrategy.getURI("tcp://localhost:6969");

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

    DistributedThrottlingStrategy strategy = new DistributedThrottlingStrategy(client);
    strategy.isThrottled("foo");
    strategy.isThrottled("bar");
    strategy.isThrottled("");

    Set<String> expected = new HashSet<String>();
    expected.add("foo");
    expected.add("bar");

    assertEquals(expected, actual);
  }
}
