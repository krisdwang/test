// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.amazon.coral.availability.DThrottlePooledClient;
import com.amazon.coral.availability.DThrottleSharedClient;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;

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

  @Test(expected = RuntimeException.class)
  public void nullSharedClient() throws Throwable {
    new DistributedThrottler((DThrottleSharedClient)null);
  }

  @Test(expected = RuntimeException.class)
  public void nullPooledClient() throws Throwable {
    new DistributedThrottler((DThrottlePooledClient)null);
  }

  @Test(expected = RuntimeException.class)
  public void nullUri() throws Throwable {
    new DistributedThrottler((String)null);
  }

  @Test(expected = RuntimeException.class)
  public void badUri() throws Throwable {
    new DistributedThrottler("@#$(^@#$");
  }

  @Test
  public void badQueryDoesNotThrottle() {
    String key = "foo";
    DThrottleSharedClient dtc = Mockito.mock(DThrottleSharedClient.class);
    Metrics metrics = Mockito.mock(Metrics.class);
    DistributedThrottler dt = new DistributedThrottler(dtc);

    Mockito.doReturn(true).when(dtc).query(Matchers.any(CharSequence.class));
    assertTrue(dt.isThrottled(key, metrics));

    RuntimeException re = new RuntimeException();
    Mockito.doThrow(re).when(dtc).query(Matchers.any(CharSequence.class));
    assertFalse(dt.isThrottled(key, metrics));
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

    @Test
    public void basicBuilderTest() {
        DistributedThrottler.Builder builder = new DistributedThrottler.Builder();
        DistributedThrottler throttler = builder.build();
        assertNotNull(throttler);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullUriBuilderTest() {
        DistributedThrottler.Builder builder = new DistributedThrottler.Builder();
        builder.setUri(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badUriBuilderTest() {
        DistributedThrottler.Builder builder = new DistributedThrottler.Builder();
        builder.setUri("A bunch of nonsense.");
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullHostBuilderTest() {
        DistributedThrottler.Builder builder = new DistributedThrottler.Builder();
        builder.setHost(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badPoolSizeBuilderTest() {
        DistributedThrottler.Builder builder = new DistributedThrottler.Builder();
        builder.setPoolSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badConnectTOBuilderTest() {
        DistributedThrottler.Builder builder = new DistributedThrottler.Builder();
        builder.setConnectTimeout(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badSoTOBuilderTest() {
        DistributedThrottler.Builder builder = new DistributedThrottler.Builder();
        builder.setSoTimeout(0);
    }

    @Test
    public void fullBuilderTest() {
        DistributedThrottler.Builder builder = new DistributedThrottler.Builder();
        builder.withUri("LocalHost:6900")
               .withBlackout(false)
               .withConnectTimeout(50)
               .withSoTimeout(20)
               .withPoolSize(30)
               .withHost("localhost")
               .withPort(1337);
        DistributedThrottler throttler = builder.build();
        assertNotNull(throttler);
    }

}
