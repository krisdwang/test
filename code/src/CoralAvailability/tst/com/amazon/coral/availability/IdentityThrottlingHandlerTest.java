package com.amazon.coral.availability;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import com.amazon.coral.service.*;
import com.amazon.coral.model.*;
import com.amazon.coral.metrics.*;
import javax.measure.unit.Unit;
import org.junit.Test;
import static org.junit.Assert.*;

public class IdentityThrottlingHandlerTest {

  private final Metrics metrics = new NullMetricsFactory().newMetrics();

  @Test(expected=RuntimeException.class)
  public void nullStrategy() {
    new IdentityThrottlingHandler(null);
  }

  @Test
  public void noIdentityToThrottleOn() {

    final double metric[] = new double[] {1, 0, 0};
    final int strategy[] = new int[] {0};

    Metrics m = new BrokenMetrics() {
      @Override
      public void addCount(String name, double value, Unit<?> unit, int repeat) {
        metric[0] = value;
        assertEquals("Throttle", name);
      }
      @Override
      public void addTime(String name, double value, Unit<?> unit, int repeat) {
        metric[1] = 1;
        assertEquals("ThrottleTime", name);
      }
      @Override
      public void addProperty(String name, CharSequence value) {
        metric[2] = 1;
        assertEquals("ThrottledKey", name);
      }
    };

    IdentityThrottlingHandler h = new IdentityThrottlingHandler(new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        strategy[0]++;
        return false;
      }
    });

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(m));
    h.before(job);

    // Check the metric emited and how many times we consulted the strategy
    assertEquals(0d, metric[0], 0);
    assertEquals(1d, metric[1], 0);
    assertEquals(0d, metric[2], 0);
    // Check if the DEFAULT was tried
    assertEquals(1, strategy[0]);

  }

  @Test
  public void identityToThrottleOn() {

    final double metric[] = new double[] {1, 0, 0};
    final int strategy[] = new int[] {0};

    Metrics m = new BrokenMetrics() {
      @Override
      public void addCount(String name, double value, Unit<?> unit, int repeat) {
        metric[0] = value;
        assertEquals("Throttle", name);
      }
      @Override
      public void addTime(String name, double value, Unit<?> unit, int repeat) {
        metric[1] = 1;
        assertEquals("ThrottleTime", name);
      }
      @Override
      public void addProperty(String name, CharSequence value) {
        metric[2] = 1;
        assertEquals("ThrottledKey", name);
      }
    };

    IdentityThrottlingHandler h = new IdentityThrottlingHandler(new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        strategy[0]++;
        return true;
      }
    });

    // Since our strategy is hard-coded above, the actual identity doesn't matter
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(m));
    IdentityHelper.setIdentity(job, identity);
    try {
      h.before(job);
      fail();
    } catch(ThrottlingException e) { }

    // Check the metric emitted and how many times we consulted the strategy
    assertEquals(1.0d, metric[0], 0);
    assertEquals(1.0d, metric[1], 0);
    assertEquals(1.0d, metric[2], 0);
    assertEquals(1, strategy[0]);

  }

  @Test
  public void badStrategyShouldNotCauseFailure() throws Throwable {
    final int[] strategy = new int[] { 0 };

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    // Since our strategy is hardcoded above, the actual identity doesn't matter
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    IdentityHelper.setIdentity(job, identity);

    IdentityThrottlingHandler h = new IdentityThrottlingHandler(new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        strategy[0]++;
        throw new UnsupportedOperationException();
      }
    });

    h.before(job);
    assertEquals(2, strategy[0]);
  }

  @Test
  public void defaultStrategyCorrectness() throws Throwable {
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));

    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    IdentityHelper.setIdentity(job, identity);

    final int[] throttleCount = new int[] { 0 };
    final String[] throttledKey = new String[] { null };

    IdentityThrottlingHandler h = new IdentityThrottlingHandler(
        new LocalThrottlingStrategy(
            new HashMap<String, String>() {{
              put(Identity.AWS_ACCOUNT+":throttled", "100");
              put("", "0");
            }}) {
              @Override
              public boolean isThrottled(CharSequence key) {
                boolean t = super.isThrottled(key);
                if(t) {
                  ++throttleCount[0];
                  throttledKey[0] = key.toString();
                }
                return t;
              }
            }
        );

    try {
      h.before(job);
    } catch(ThrottlingException t) {}

    assertEquals(1, throttleCount[0]);
    assertNotNull(throttledKey[0]);
    assertEquals("", throttledKey[0]);
  }

  @Test
  public void nonDefaultStrategyCorrectness() throws Throwable {
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));

    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    IdentityHelper.setIdentity(job, identity);

    final int[] throttleCount = new int[] { 0 };
    final String[] throttledKey = new String[] { null };

    IdentityThrottlingHandler h = new IdentityThrottlingHandler(
        new LocalThrottlingStrategy(
            new HashMap<String, String>() {{
              put(Identity.AWS_ACCOUNT+":throttled", "0");
              put("", "100");
            }}) {
              @Override
              public boolean isThrottled(CharSequence key) {
                boolean t = super.isThrottled(key);
                if(t) {
                  ++throttleCount[0];
                  throttledKey[0] = key.toString();
                }
                return t;
              }
            }
        );

    try {
      h.before(job);
    } catch(ThrottlingException t) {}

    assertEquals(1, throttleCount[0]);
    assertNotNull(throttledKey[0]);
    assertEquals(Identity.AWS_ACCOUNT+":throttled", throttledKey[0]);
  }

  //@Test // this now warns instead
  public void noDefaultExemptions() {

    final int[] exempt = new int[] { 0 };
    final int[] strategy = new int[] { 0 };

    ThrottlingStrategy s = new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        strategy[0]++;
        return false;
      }
    };

    // A handler that considers any identity exempt
    IdentityThrottlingHandler h = new IdentityThrottlingHandler(s) {
      @Override
      public boolean isExempt(CharSequence key) {
        exempt[0]++;
        return true;
      }
    };

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    h.before(job);

    // Check that we did not exempt an id, and that we did engage the throttler
    assertEquals(0, exempt[0]);
    assertEquals(1, strategy[0]);

  }

  @Test
  public void exemptions() {

    final int[] exempt = new int[] { 0 };
    final int[] strategy = new int[] { 0 };

    ThrottlingStrategy s = new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        strategy[0]++;
        return false;
      }
    };

    // A handler that considers any identity exempt
    IdentityThrottlingHandler h = new IdentityThrottlingHandler(s) {
      @Override
      public boolean isExempt(CharSequence key) {
        if((Identity.AWS_ACCOUNT+":bar").contentEquals(key)) {
          exempt[0]++;
          return true;
        }
        return false;
      }
    };

    // Job w/ exempted id
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT,"bar");
    h.before(job);

    // Job w/o exempted id
    job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    h.before(job);

    // Check that we did exempt an id, and that we did engage the throttler
    // for another id
    assertEquals(1, exempt[0]);
    assertEquals(1, strategy[0]);

  }

  @Test
  public void ignoredKeys() {
    // Ensure only the "sane" identity keys are being used to throttle on
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add(Identity.AWS_ACCOUNT+":throttled");
    expectedKeys.add(Identity.HTTP_REMOTE_ADDRESS+":throttled");
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityThrottlingHandler(strategy);

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "throttled");
    identity.setAttribute(Identity.HTTP_USER_AGENT, "who-cares");
    identity.setAttribute("foo", "bar");

    h.before(job);

    assertEquals(expectedKeys, new HashSet<String>(strategy.getSeenKeys()));
  }

  @Test
  public void specifiedKeys() {
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add(Identity.AWS_ACCOUNT+":throttled");
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityThrottlingHandler(Arrays.<String>asList(Identity.AWS_ACCOUNT), strategy);

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "0.0.0.0");
    identity.setAttribute(Identity.HTTP_USER_AGENT, "Mosaic");
    identity.setAttribute(Identity.AWS_ACCESS_KEY, "foo");

    h.before(job);

    assertEquals(expectedKeys, new HashSet<String>(strategy.getSeenKeys()));
  }

  @Test
  public void prefix() {
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add("ID:"+Identity.AWS_ACCOUNT+":throttled");
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityThrottlingHandler(Arrays.<String>asList(Identity.AWS_ACCOUNT), true, strategy);

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "0.0.0.0");
    identity.setAttribute(Identity.HTTP_USER_AGENT, "Mosaic");
    identity.setAttribute(Identity.AWS_ACCESS_KEY, "foo");

    h.before(job);

    assertEquals(expectedKeys, new HashSet<String>(strategy.getSeenKeys()));
  }
}
