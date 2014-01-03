// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.flow;

import org.mockito.Mockito;
import org.mockito.Matchers;

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
import java.util.Collection;

import javax.measure.unit.Unit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

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
import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.avail.throttlers.ThrottlerRetriever;
import com.amazon.coral.avail.throttlers.ThrottlerMemoizer;
import com.amazon.coral.avail.throttlers.LocalThrottler;
import com.amazon.coral.availability.ThrottlingException;

import com.amazon.coral.avail.key.*;

// local
import com.amazon.coral.service.MyHandlerContext;
import com.amazon.coral.avail.Helper;


public class ThrottlingHandlerTest {

  private final Metrics metrics = new NullMetricsFactory().newMetrics();
  private ThrottlingHandler.Builder bldr;

  private final static Throttler alwaysThrottler = new Throttler() {
    @Override
    public boolean isThrottled(CharSequence cs, Metrics m) {
      return true;
    }
  };

  private final static KeyGenerator nullGenerator = new KeyGenerator() {
    @Override
    public Iterable<CharSequence> getKeys(HandlerContext ctx) {
      return java.util.Collections.<CharSequence>emptyList();
    }
  };

  private final static java.util.Collection<CharSequence> emptyList = java.util.Collections.<CharSequence>emptyList();
  private final static ThrottlerRetriever alwaysRetriever = new ThrottlerRetriever(alwaysThrottler);

  @Before
  public void setUp() {
    ThrottlingHandler.Builder bldr = ThrottlingHandler.newBuilder();
  }

  @Test
  public void nullGenerator() {
    try {
      new ThrottlingHandler((KeyGenerator)null, alwaysRetriever, emptyList);
      fail("should throw");
    } catch (RuntimeException e) {
      Assert.assertEquals("null KeyGenerator", e.getMessage());
    }
  }

  @Test
  public void nullRetriever() {
    try {
      new ThrottlingHandler(nullGenerator, null, emptyList);
      fail("should throw");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("null retriever"));
    }
  }

  @Test
  public void getters() {
    ThrottlingHandler h;
    h = new ThrottlingHandler(nullGenerator, alwaysRetriever, emptyList);
    assertEquals(nullGenerator, h.getGenerator());
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


    ThrottlingHandler h = new ThrottlingHandler(
        KeyGeneratorBuilder.newBuilderForThrottlingHandler().buildForIdentity(),
        new ThrottlerRetriever(new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics m) {
        strategy[0]++;
        return false;
      }
    }), emptyList);

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(m));
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);
    h.before(ctx);

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

    ThrottlingHandler.Builder bldr = new ThrottlingHandler.Builder();
    bldr.setThrottler(new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics m) {
        strategy[0]++;
        return true;
      }
    });
    bldr.setKeyGenerator(new IdentityKeyGenerator());
    ThrottlingHandler h = bldr.build();

    // Since our strategy is hard-coded above, the actual identity doesn't matter
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(m));
    IdentityHelper.setIdentity(job, identity);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);
    try {
      h.before(ctx);
      fail("should throw");
    } catch(com.amazon.coral.availability.ThrottlingException e) { }

    // Check the metric emitted and how many times we consulted the strategy
    assertEquals(1.0d, metric[0], 0);
    assertEquals(1.0d, metric[1], 0);
    assertEquals(1, strategy[0]);

  }

  @Test
  public void badKeyGeneratorShouldNotCauseFailure() throws Throwable {
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Metrics metrics = new NullMetricsFactory().newMetrics();
    Mockito.doReturn(metrics).when(ctx).getMetrics();

    KeyGenerator kg = new KeyGenerator() {
      @Override
      public Iterable<CharSequence> getKeys(HandlerContext ctx) {
        throw new UnsupportedOperationException();
      }
    };

    ThrottlingHandler.Builder bldr = new ThrottlingHandler.Builder();
    Throttler t = Mockito.mock(Throttler.class);
    bldr.setThrottler(t);
    bldr.setKeyGenerator(kg);
    ThrottlingHandler h = bldr.build();

    h.before(ctx);
    // there will be no generated keys to check
    Mockito.verifyNoMoreInteractions(t);
  }

  @Test
  public void badStrategyShouldNotCauseFailure() throws Throwable {
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Metrics metrics = new NullMetricsFactory().newMetrics();
    Mockito.doReturn(metrics).when(ctx).getMetrics();

    ThrottlingHandler.Builder bldr = new ThrottlingHandler.Builder();
    Throttler t = Mockito.mock(Throttler.class);
    bldr.setThrottler(t);
    Exception exc = new UnsupportedOperationException();
    Mockito.doThrow(exc).when(t).isThrottled(Matchers.any(CharSequence.class), Matchers.any(Metrics.class));
    bldr.setKeyGenerator(new StaticKeyGenerator("foo"));
    ThrottlingHandler h = bldr.build();

    h.before(ctx);
  }

  @Test
  public void defaultStrategyCorrectness() throws Throwable {
    // Actually, this tests a bunch of stuff.
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Metrics metrics = new NullMetricsFactory().newMetrics();
    Mockito.doReturn(metrics).when(ctx).getMetrics();

    final String expectedKey = Identity.AWS_ACCOUNT + ":throttled";
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    Mockito.doReturn(identity).when(ctx).getIdentity();

    final int[] throttleCount = new int[] { 0 };
    final String[] throttledKey = new String[] { null };

    ThrottlingHandler.Builder bldr = new ThrottlingHandler.Builder();
    Throttler throttler = Mockito.mock(Throttler.class);
    Mockito.doReturn(true).when(throttler).isThrottled(ThrottlingHandler.DEFAULT_KEY, metrics);
    bldr.setThrottler(throttler);
    bldr.setKeyGenerator(KeyGeneratorBuilder.newBuilderForThrottlingHandler().buildForIdentity());
    ThrottlingHandler h = bldr.build();

    try {
      h.before(ctx);
      fail("should throw");
    } catch(com.amazon.coral.availability.ThrottlingException t) {}

    com.amazon.coral.avail.Helper.verifyKeys(throttler, "");
  }

  @Test
  public void nonDefaultStrategyCorrectness() throws Throwable {
    // Actually, this tests a bunch of stuff.
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Metrics metrics = new NullMetricsFactory().newMetrics();
    Mockito.doReturn(metrics).when(ctx).getMetrics();

    final String expectedKey = Identity.AWS_ACCOUNT + ":throttled";
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    Mockito.doReturn(identity).when(ctx).getIdentity();

    final int[] throttleCount = new int[] { 0 };
    final String[] throttledKey = new String[] { null };

    ThrottlingHandler.Builder bldr = new ThrottlingHandler.Builder();
    Throttler throttler = Mockito.mock(Throttler.class);
    Mockito.doReturn(true).when(throttler).isThrottled(Helper.matchedString(expectedKey), Matchers.same(metrics));
    bldr.setThrottler(throttler);
    bldr.setKeyGenerator(new IdentityKeyGenerator());
    ThrottlingHandler h = bldr.build();

    try {
      h.before(ctx);
      fail("should throw");
    } catch(com.amazon.coral.availability.ThrottlingException t) {}

    com.amazon.coral.avail.Helper.verifyKeys(throttler, expectedKey);
  }

  @Test
  public void multipleKeysOneThrottled() {
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Metrics metrics = new NullMetricsFactory().newMetrics();
    Mockito.doReturn(metrics).when(ctx).getMetrics();

    Throttler throttler = Mockito.mock(Throttler.class);
    Mockito.doReturn(false).when(throttler).isThrottled(Matchers.any(CharSequence.class), Matchers.any(Metrics.class));
    Mockito.doReturn(true).when(throttler).isThrottled(com.amazon.coral.avail.Helper.matchedString("b-throttled"), Matchers.any(Metrics.class));

    ThrottlingHandler h = new ThrottlingHandler(
        new StaticKeyGenerator("a", "b-throttled", "c"),
        new ThrottlerRetriever(throttler),
        emptyList);

    try {
      h.before(ctx);
      fail("should throw");
    } catch(com.amazon.coral.availability.ThrottlingException t) {}

    com.amazon.coral.avail.Helper.verifyKeys(throttler, "a", "b-throttled"); // no "c"
  }

  @Test
  public void multipleKeysNotThrottled() throws com.amazon.coral.availability.ThrottlingException {
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Metrics metrics = new NullMetricsFactory().newMetrics();
    Mockito.doReturn(metrics).when(ctx).getMetrics();

    Throttler throttler = Mockito.mock(Throttler.class);
    Mockito.doReturn(false).when(throttler).isThrottled(Matchers.any(CharSequence.class), Matchers.any(Metrics.class));

    ThrottlingHandler h = new ThrottlingHandler(
        new StaticKeyGenerator("aws-account:throttled", ""),
        new ThrottlerRetriever(throttler),
        Collections.<CharSequence>emptyList());

    h.before(ctx);

    com.amazon.coral.avail.Helper.verifyKeys(throttler, "aws-account:throttled", "");
  }

  @Test
  public void exemptionsButNoMatchingKeys() {
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Metrics metrics = new NullMetricsFactory().newMetrics();
    Mockito.doReturn(metrics).when(ctx).getMetrics();

    Throttler throttler = Mockito.mock(Throttler.class);
    Mockito.doReturn(false).when(throttler).isThrottled(Matchers.any(CharSequence.class), Matchers.any(Metrics.class));

    Collection<CharSequence> exemptions = Arrays.asList((CharSequence)"foo-is-exempt");

    ThrottlingHandler h = new ThrottlingHandler(
        new StaticKeyGenerator("bar", "baz"),
        new ThrottlerRetriever(throttler),
        exemptions);
    h.before(ctx);
    com.amazon.coral.avail.Helper.verifyKeys(throttler, "bar", "baz");
  }

  @Test
  public void noThrottlingForExemption() {
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Metrics metrics = new NullMetricsFactory().newMetrics();
    Mockito.doReturn(metrics).when(ctx).getMetrics();

    Throttler throttler = Mockito.mock(Throttler.class);
    Mockito.doReturn(true).when(throttler).isThrottled(Matchers.any(CharSequence.class), Matchers.any(Metrics.class));

    Collection<CharSequence> exemptions = Arrays.asList((CharSequence)"foo-is-exempt");

    ThrottlingHandler h = new ThrottlingHandler(
        new StaticKeyGenerator("bar", "foo-is-exempt", "baz"),
        new ThrottlerRetriever(throttler),
        exemptions);
    h.before(ctx);
    com.amazon.coral.avail.Helper.verifyKeys(throttler); // none!
  }

  @Test
  public void noDuplicateThrottleCalls() {
    HandlerContext ctx = MyHandlerContext.create(true); // for UserState

    Throttler throttler = Mockito.mock(Throttler.class);
    Mockito.doReturn(false).when(throttler).isThrottled(Matchers.any(CharSequence.class), Matchers.any(Metrics.class));

    ThrottlingHandler.Builder b = new ThrottlingHandler.Builder();
    b.setThrottler(throttler); // memoized by default

    b.setKeyGenerator(new StaticKeyGenerator("key1", "key2"));
    ThrottlingHandler handler = b.build();

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add(ThrottlingHandler.DEFAULT_KEY);

    handler.before(ctx);
    com.amazon.coral.avail.Helper.verifyKeys(throttler, "key1", "key2");

    handler.before(ctx);
    Mockito.verifyNoMoreInteractions(throttler);
  }

  @Test
  public void yesDuplicateThrottleCalls() {
    // No memoizer, so isThrottled() calls will be repeated.
    HandlerContext ctx = MyHandlerContext.create(true); // for UserState

    Throttler throttler = Mockito.mock(Throttler.class);
    Mockito.doReturn(false).when(throttler).isThrottled(Matchers.any(CharSequence.class), Matchers.any(Metrics.class));

    ThrottlingHandler.Builder b = new ThrottlingHandler.Builder();
    b.setThrottlerUnmemoized(throttler); // memoized by default

    b.setKeyGenerator(new StaticKeyGenerator("key1", "key2"));
    ThrottlingHandler handler = b.build();

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add(ThrottlingHandler.DEFAULT_KEY);

    handler.before(ctx);
    handler.before(ctx);
    com.amazon.coral.avail.Helper.verifyKeys(throttler, "key1", "key2", "key1", "key2");
  }

  @Test
  public void builderSetters() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();

    assertNull(b.retriever);
    Throttler s = new Throttler() {
      public boolean isThrottled(CharSequence key, Metrics m) {return false;}
    };
    b.setThrottler(s);
    assertNotNull(b.retriever);

    java.util.Collection<CharSequence> el = new java.util.ArrayList<CharSequence>();
    assertNotNull(b.exemptions);
    assertEquals(el, b.exemptions);
    assertNotSame(el, b.exemptions);
    b.setExemptions(el);
    assertEquals(el, b.exemptions);
    assertSame(el, b.exemptions);

    assertNull(b.keygen);
    b.setKeyGenerator(new StaticKeyGenerator(new String[]{}));
    assertNotNull(b.keygen);
  }

  @Test
  public void builderWithers() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    ThrottlingHandler.Builder r = null;
    assertNotSame(b, r);

    assertNull(b.retriever);
    Throttler s = new Throttler() {
      public boolean isThrottled(CharSequence key, Metrics m) {return false;}
    };
    r = b.withThrottler(s);
    assertSame(b, r);
    assertNotNull(b.retriever);
    assertSame(ThrottlerMemoizer.class, b.retriever.getClass());

    r = b.withThrottlerUnmemoized(s);
    assertSame(b, r);
    assertNotNull(b.retriever);
    assertSame(ThrottlerRetriever.class, b.retriever.getClass());

    ThrottlerRetriever tr = new ThrottlerRetriever(s);
    r = b.withThrottlerRetriever(tr);
    assertSame(b, r);
    assertSame(tr, b.retriever);

    java.util.Collection<CharSequence> el = new java.util.ArrayList<CharSequence>();
    assertNotNull(b.exemptions);
    assertEquals(el, b.exemptions);
    assertNotSame(el, b.exemptions);
    r = b.withExemptions(el);
    assertSame(b, r);
    assertEquals(el, b.exemptions);
    assertSame(el, b.exemptions);
  }

  @Test(expected=RuntimeException.class)
  public void builderNothingSet() {
    ThrottlingHandler.Builder b = ThrottlingHandler.newBuilder();
    b.build();
  }

  @Test
  public void builderKeyGenerator() {
    ThrottlingHandler.Builder b = new ThrottlingHandler.Builder();
    b.setThrottler(alwaysThrottler);
    b.setKeyGenerator(new StaticKeyGenerator(new String[]{}));
    b.build();
  }

  @Test
  public void tryNoOpThrottlingHandler() {
    // not sure anybody uses this, but a quick test cannot hurt
    try {
      NoOpThrottlingHandler h = new NoOpThrottlingHandler(null);
      Assert.fail("should throw");
    } catch (RuntimeException e) {}

    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    ThrottlingHandler th = Mockito.mock(ThrottlingHandler.class);
    ThrottlingException te = new ThrottlingException();
    RuntimeException re = new RuntimeException();
    NoOpThrottlingHandler h = new NoOpThrottlingHandler(th);

    h.before(ctx);
    Mockito.verify(th).before(ctx);

    Mockito.doThrow(te).when(th).before(ctx);
    h.before(ctx);

    Mockito.doThrow(re).when(th).before(ctx);
    try {
      h.before(ctx);
    } catch (RuntimeException e) {
      Assert.assertSame(re, e);
    }
  }
}
