// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;

import javax.measure.unit.Unit;

import org.junit.Test;

import com.amazon.coral.metrics.BrokenMetrics;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.model.NullModelIndex;
import com.amazon.coral.service.Identity;
import com.amazon.coral.service.IdentityHelper;
import com.amazon.coral.service.Job;
import com.amazon.coral.service.JobImpl;
import com.amazon.coral.service.RequestImpl;
import com.amazon.coral.throttle.api.Throttler;

public class ThrottlingHandlerTest {

  private final Metrics metrics = new NullMetricsFactory().newMetrics();

  private final static ThrottlingStrategy alwaysThrottle = new ThrottlingStrategy() {
    @Override
    public boolean isThrottled(CharSequence cs) {
      return true;
    }
  };

  private final static Throttler alwaysThrottler = new Throttler() {
    @Override
    public boolean isThrottled(CharSequence cs, Metrics m) {
      return true;
    }
  };

  private final static KeyBuilder nullBuilder = new KeyBuilder() {
    @Override
    public Iterable<CharSequence> getKeys(Job job) {
      return java.util.Collections.<CharSequence>emptyList();
    }
  };

  @Test(expected=RuntimeException.class)
  public void nullBuilder() {
    new ThrottlingHandler(null, alwaysThrottle);
  }

  @Test(expected=RuntimeException.class)
  public void nullStrategy() {
    new ThrottlingHandler(nullBuilder, (ThrottlingStrategy)null);
  }

  @Test(expected=NullPointerException.class)
  public void nullThrottler() {
    new ThrottlingHandler(nullBuilder, (Throttler)null,
      Collections.<CharSequence>emptyList());
  }

  @Test
  public void getters() {
    ThrottlingHandler h = new ThrottlingHandler(nullBuilder, alwaysThrottle);
    assertEquals(nullBuilder, h.getBuilder());
    assertEquals(alwaysThrottle, h.getStrategy());
    try {
      h.getExemptions().add("foo");
      fail();
    } catch (UnsupportedOperationException e) {}
  }

  @Test
  public void noIdentityToThrottleOn() {

    final double metric[] = new double[] {1, 0};
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
    };

    ThrottlingHandler h = new ThrottlingHandler(new IdentityKeyBuilder(), new ThrottlingStrategy() {
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
    // Check if the DEFAULT was tried
    assertEquals(1, strategy[0]);

  }

  @Test
  public void identityToThrottleOn() {

    final double metric[] = new double[] {1, 0};
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
      }
    };

    ThrottlingHandler h = new ThrottlingHandler(new IdentityKeyBuilder(), new ThrottlingStrategy() {
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
    assertEquals(1, strategy[0]);

  }

  @Test
  public void badKeyBuliderShouldNotCauseFailure() throws Throwable {
    final int[] strategy = new int[] { 0 };

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    // Since our strategy is hardcoded above, the actual identity doesn't matter
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    IdentityHelper.setIdentity(job, identity);

    KeyBuilder builder = new KeyBuilder() {
      @Override
      public Iterable<CharSequence> getKeys(Job job) {
        throw new UnsupportedOperationException();
      }
    };

    ThrottlingHandler h = new ThrottlingHandler(builder, new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        strategy[0]++;
        return false;
      }
    });

    h.before(job);
    // Should check the default keys, but there will be no generated keys to check
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

    ThrottlingHandler h = new ThrottlingHandler(new IdentityKeyBuilder(), new ThrottlingStrategy() {
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

    final String expectedKey = Identity.AWS_ACCOUNT + ":throttled";
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    IdentityHelper.setIdentity(job, identity);

    final int[] throttleCount = new int[] { 0 };
    final String[] throttledKey = new String[] { null };

    ThrottlingHandler h = new ThrottlingHandler(
        new IdentityKeyBuilder(),
        new LocalThrottlingStrategy(
            new HashMap<String, String>() {{
              put(expectedKey, "100");
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

    final String expectedKey = Identity.AWS_ACCOUNT + ":throttled";
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    IdentityHelper.setIdentity(job, identity);

    final int[] throttleCount = new int[] { 0 };
    final String[] throttledKey = new String[] { null };

    ThrottlingHandler h = new ThrottlingHandler(
        new IdentityKeyBuilder(),
        new LocalThrottlingStrategy(
            new HashMap<String, String>() {{
              put(expectedKey, "0");
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
    assertEquals(expectedKey, throttledKey[0]);
  }

  @Test
  public void isStrategyDefaultThrottled() throws Throwable {
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));

    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    IdentityHelper.setIdentity(job, identity);

    final boolean[] defaultKeyChecked = new boolean[1];
    ThrottlingStrategy strategy = new ThrottlingStrategy() {

      @Override
      public boolean isThrottled(CharSequence key) {
        if(key == ThrottlingHandler.DEFAULT_KEY) {
          defaultKeyChecked[0] = true;
        }
        return false;
      }
    };

    ThrottlingHandler h = new ThrottlingHandler(new IdentityKeyBuilder(), strategy);

    try {
      h.before(job);
    } catch(ThrottlingException t) {}

    assertTrue(defaultKeyChecked[0]);
  }

  @Test
  public void throttlerCorrectness() throws Throwable {
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));

    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    IdentityHelper.setIdentity(job, identity);

    final Throttler[] activatedThrottlers = new Throttler[1];
    Throttler throttler = new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics metrics) {
        activatedThrottlers[0] = this;
        return false;
      }
    };

    ThrottlingHandler h = new ThrottlingHandler(new IdentityKeyBuilder(), throttler,
      Collections.<CharSequence>emptyList());

    try {
      h.before(job);
    } catch(ThrottlingException t) {}

    assertEquals(throttler, activatedThrottlers[0]);
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
    ThrottlingHandler h = new ThrottlingHandler(new IdentityKeyBuilder(), s) {
      @Override
      public boolean isExempt(CharSequence key) {
        exempt[0]++;
        return true;
      }
    };

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    h.before(job);

    // Check that we did exempt an id, and that we did engage the throttler
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
    ThrottlingHandler h = new ThrottlingHandler(new IdentityKeyBuilder(), s) {
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
    identity.setAttribute(Identity.AWS_ACCOUNT, "bar");
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
  public void noThrottlingForExemption() {
    final int[] strategyCalls = new int[] { 0 };

    // Ensure that we check all the keys for exemptions before attempting throttling
    ThrottlingStrategy strategy = new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        strategyCalls[0]++;
        return true;
      }
    };

    KeyBuilder builder = new KeyBuilder() {
      @Override
      public Iterable<CharSequence> getKeys(Job job) {
        List<CharSequence> keys = new LinkedList<CharSequence>();
        keys.add("foo");
        keys.add("bar");
        keys.add("baz");
        return keys;
      }
    };

    List<CharSequence> exemptions = new LinkedList<CharSequence>();
    exemptions.add("baz"); // The last key returned is the exempt one, but we will still exempt the entire request

    ThrottlingHandler handler = new ThrottlingHandler(builder, strategy, exemptions);

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    handler.before(job);

    assertEquals(0, strategyCalls[0]);
  }

  @Test
  public void noDuplicateThrottleCalls() {
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler handler = new ThrottlingHandler(new IdentityKeyBuilder(), strategy);

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(metrics));
    Identity identity = IdentityHelper.getIdentity(job);

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add("");

    handler.before(job);
    assertEquals(expectedKeys, strategy.getSeenKeys());

    identity.setAttribute(Identity.AWS_ACCOUNT, "1");
    expectedKeys.add("aws-account:1");
    handler.before(job);
    assertEquals(expectedKeys, strategy.getSeenKeys());

    identity.setAttribute(Identity.AWS_ACCESS_KEY, "2");
    expectedKeys.add("aws-access-key:2");
    handler.before(job);
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void builderSetters() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();

    assertNull(b.keys);
    b.setKeys(new java.util.ArrayList<String>());
    assertNotNull(b.keys);

    assertNull(b.throttler);
    ThrottlingStrategy s = new ThrottlingStrategy() {
      public boolean isThrottled(CharSequence key) {return false;}
    };
    b.setStrategy(s);
    assertNotNull(b.throttler);

    java.util.Collection<CharSequence> el = new java.util.ArrayList<CharSequence>();
    assertNotNull(b.exemptions);
    assertEquals(el, b.exemptions);
    assertNotSame(el, b.exemptions);
    b.setExemptions(el);
    assertEquals(el, b.exemptions);
    assertSame(el, b.exemptions);

    assertFalse(b.usePrefix);
    b.setUsePrefix(true);
    assertTrue(b.usePrefix);
    b.setUsePrefix(false);
    assertFalse(b.usePrefix);

    assertNull(b.cls);
    b.setType(ThrottlingHandler.Type.OPERATION);
    assertEquals(ThrottlingHandler.Type.OPERATION, b.cls);

    assertNull(b.keyBuilder);
    b.setKeyBuilder(new StaticKeyBuilder(new String[]{}));
    assertNotNull(b.keyBuilder);
  }

  @Test
  public void builderWithers() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    ThrottlingHandler.Builder r = null;
    assertNotSame(b, r);

    assertNull(b.keys);
    r = b.withKeys(new java.util.ArrayList<String>());
    assertSame(b, r);
    assertNotNull(b.keys);

    assertNull(b.throttler);
    ThrottlingStrategy s = new ThrottlingStrategy() {
      public boolean isThrottled(CharSequence key) {return false;}
    };
    r = b.withStrategy(s);
    assertSame(b, r);
    assertNotNull(b.throttler);

    java.util.Collection<CharSequence> el = new java.util.ArrayList<CharSequence>();
    assertNotNull(b.exemptions);
    assertEquals(el, b.exemptions);
    assertNotSame(el, b.exemptions);
    r = b.withExemptions(el);
    assertSame(b, r);
    assertEquals(el, b.exemptions);
    assertSame(el, b.exemptions);

    assertFalse(b.usePrefix);
    r = b.withUsePrefix(true);
    assertSame(b, r);
    assertTrue(b.usePrefix);
    r = b.withUsePrefix(false);
    assertSame(b, r);
    assertFalse(b.usePrefix);

    assertNull(b.cls);
    r = b.withType(ThrottlingHandler.Type.IDENTITY);
    assertSame(b, r);
    assertEquals(ThrottlingHandler.Type.IDENTITY, b.cls);
  }

  @Test
  public void builderType() {
    ThrottlingHandler h;
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    b.setThrottler(alwaysThrottler);

    b.setType(ThrottlingHandler.Type.IDENTITY);
    h = b.build();
    assertEquals(IdentityThrottlingHandler.class, h.getClass());

    b.setType(ThrottlingHandler.Type.IDENTITY_OPERATION);
    h = b.build();
    assertEquals(IdentityOperationThrottlingHandler.class, h.getClass());

    b.setType(ThrottlingHandler.Type.OPERATION_IDENTITY);
    h = b.build();
    assertEquals(OperationIdentityThrottlingHandler.class, h.getClass());

    b.setType(ThrottlingHandler.Type.OPERATION);
    h = b.build();
    assertEquals(OperationThrottlingHandler.class, h.getClass());

    b.setType(ThrottlingHandler.Type.UNKNOWN);
    try {
      h = b.build();
      fail("should throw");
    } catch (RuntimeException e) {
    }
  }

  @Test(expected=RuntimeException.class)
  public void builderNothingSet() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    b.build();
  }

  @Test
  public void builderTypePlusKeyBuilder() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    b.setType(ThrottlingHandler.Type.OPERATION);
    b.setKeyBuilder(new StaticKeyBuilder(new String[]{}));
    try {
      b.build();
      fail("should throw");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Either type or keyBuilder must be set, but not both"));
    }
  }

  @Test
  public void builderKeysPlusKeyBuilder() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    b.setKeys(Arrays.asList(new String[]{}));
    b.setKeyBuilder(new StaticKeyBuilder(new String[]{}));
    try {
      b.build();
      fail("should throw");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("keys cannot be supplied with keyBuilder"));
    }
  }

  @Test
  public void builderKeyBuilder() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    b.setThrottler(alwaysThrottler);
    b.setKeyBuilder(new StaticKeyBuilder(new String[]{}));
    b.build();
  }

  /* Currently, memoizing is always on. */
  //@Test
  public void unmemoizedThrottler() {
    final int count[] = new int[]{0};

    Throttler s = new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics metrics) {
        count[0]++;
        return true;
      }
    };
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    b.setThrottler(s);
    b.setKeyBuilder(new StaticKeyBuilder(new String[]{}));
    ThrottlingHandler h = b.build();

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(new NullMetricsFactory().newMetrics()));
    s = h.getThrottler(job);

    assertTrue( s.isThrottled("a", null) );
    assertTrue( s.isThrottled("a", null) );
    assertTrue( s.isThrottled("a", null) );
    assertEquals(count[0], 3);

    assertTrue( s.isThrottled("", null) );
    assertTrue( s.isThrottled("", null) );
    assertTrue( s.isThrottled("", null) );
    assertEquals(count[0], 6);
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
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    b.setThrottler(s);
    b.setKeyBuilder(new StaticKeyBuilder(new String[]{}));
    ThrottlingHandler h = b.build();

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(new NullMetricsFactory().newMetrics()));
    s = h.getThrottler(job);

    assertTrue( s.isThrottled("a", null) );
    assertTrue( s.isThrottled("a", null) );
    assertTrue( s.isThrottled("a", null) );
    assertEquals(count[0], 1);

    assertTrue( s.isThrottled("", null) );
    assertTrue( s.isThrottled("", null) );
    assertTrue( s.isThrottled("", null) );
    assertEquals(count[0], 2);
  }

}
