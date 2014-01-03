// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.congestion;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.measure.unit.Unit;

import com.amazon.coral.metrics.*;
import com.amazon.coral.service.*;
import com.amazon.coral.model.NullModelIndex;
import com.amazon.coral.throttle.api.Throttler;
import static com.amazon.coral.avail.Helper.*;
import com.amazon.coral.avail.key.*;

import com.amazon.coral.avail.throttlers.ThrottlerRetriever;
import com.amazon.coral.avail.throttlers.ThrottlerMemoizer;

// local
import com.amazon.coral.avail.TrackedMetrics;
import com.amazon.coral.avail.throttlers.TrackedThrottler;
import com.amazon.coral.avail.throttlers.StaticThrottler;


import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class LoadShedHandlerTest {
  LoadShedHandler.Builder newLoadShedHandlerBuilder(KeyGenerator kg, int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setKeyGenerator(kg);
    b.setCapacity(capacity);
    b.setThrottler(throttler);
    b.setExemptions(exemptions);
    return b;
  }

  @Test
  public void nullKeyGenerator() {
    KeyGenerator kg = null;
    Throttler strategy = newBlacklistThrottler();
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();
    try {
      newLoadShedHandlerBuilder(kg, 0, strategy, exemptions).build();
    } catch (RuntimeException e) {
      assertEquals("failed to set KeyGenerator", e.getMessage());
    }
  }

  @Test
  public void nullGenerator4Ctor() {
    KeyGenerator kg = null;
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();
    try {
      new LoadShedHandler(kg, 0, new ThrottlerRetriever(newBlacklistThrottler()), exemptions);
    } catch (RuntimeException e) {
      assertEquals("null KeyGenerator", e.getMessage());
    }
  }

  @Test
  public void nullThrottlerRetriever() {
    KeyGenerator kg = new CartesianProductKeyGenerator(Collections.<KeyGenerator>emptyList(), ",");
    Throttler strategy = null;
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.emptyList();
    try {
      new LoadShedHandler.Builder().withKeyGenerator(kg).withCapacity(0).withExemptions(exemptions).build();
    } catch (RuntimeException e) {
      assertEquals("failed to set LoadShedHandler throttler", e.getMessage());
    }
  }

  @Test
  public void negativeCapacity() {
    KeyGenerator kg = new CartesianProductKeyGenerator(Collections.<KeyGenerator>emptyList(), ",");
    Throttler strategy = newBlacklistThrottler();
    int capacity = -1;
    Collection<CharSequence> exemptions = Collections.emptyList();
    try {
      newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();
    } catch (RuntimeException e) {
      assertEquals("negative capacity", e.getMessage());
    }
  }

  @Test
  public void nullExemptions() {
    KeyGenerator kg = new CartesianProductKeyGenerator(Collections.<KeyGenerator>emptyList(), ",");
    Throttler strategy = newBlacklistThrottler();
    int capacity = 0;
    Collection<CharSequence> exemptions = null;
    try {
      newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();
    } catch (RuntimeException e) {
      assertEquals("null exemptions", e.getMessage());
    }
  }

  @Test
  public void defaultKeyIsChecked() throws Throwable {
    KeyGenerator kg = KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler();
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();

    Job job = newJob(0);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);
    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable" });

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void absenceOfGlobalKey() throws Throwable {
    KeyGenerator kg = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withGlobalKey(null).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler();
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();

    Job job = newJob(0);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);
    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] {});

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void keysAreProperlyPrefixed() throws Throwable {
    KeyGenerator lkg = new StaticKeyGenerator("foo", "bar");
    KeyGenerator kg1 = new PrefixKeyGenerator("droppable", "-", lkg);
    KeyGenerator kg2 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler();
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler1 = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions).build();
    UserHandler handler2 = newLoadShedHandlerBuilder(kg2, capacity, strategy, exemptions).build();

    Job job = newJob(0);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);
    handler1.before(ctx);
    handler2.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-foo", "droppable-bar", "droppable" });

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noSheddingUnderCapacity() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler();
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler1 = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions).build();

    Job job = newJob(1);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler1.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:foo", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noSheddingAtCapacity() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler();
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler1 = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions).build();

    Job job = newJob(2);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler1.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:foo", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noSheddingWithoutThrottlingAtCapacity() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler();
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler1 = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions).build();

    Job job = newJob(3);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler1.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:foo", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void sheddingWithThrottlingAtCapacity() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable-aws-account:foo"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions).build();

    Job job = newJob(3);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    ServiceUnavailableException lse = null;
    try {
      handler.before(ctx);
    } catch (ServiceUnavailableException e) {
      lse = e;
    }
    assertNotNull(lse);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:foo" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noSheddingWithoutRequestCount() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable-aws-account:foo"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions).build();

    Job job = newJob(0);
    RequestCountHelper.setCount(job, null);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler.before(ctx);

    // The throttling checks are short-circuited if the request count isn't available.
    List<String> expectedKeys = Arrays.asList(new String[] {});
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void keyBuilderFailure() throws Throwable {
    KeyGenerator kg = new KeyGenerator() {
      @Override
      public Iterable<CharSequence> getKeys(HandlerContext ctx) {
        throw new RuntimeException();
      }
    };
    TrackedThrottler strategy = new TrackedThrottler();
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();

    Job job = newJob(1);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler.before(ctx);
  }

  @Test
  public void badThrottlerNotCalledWithNoKeys() throws Throwable {
    KeyGenerator kg = new StaticKeyGenerator();
    TrackedThrottler strategy = new TrackedThrottler(new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics m) {
        throw new RuntimeException();
      }
    });
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();

    Job job = newJob(1);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[0]);
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void badThrottlerNotShed() throws Throwable {
    KeyGenerator kg = new StaticKeyGenerator("foo");
    TrackedThrottler strategy = new TrackedThrottler(new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics m) {
        throw new RuntimeException();
      }
    });
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();

    Job job = newJob(1);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList("foo");
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noExemptShedding() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentities(Arrays.asList(new String[] { Identity.AWS_ACCOUNT, Identity.HTTP_REMOTE_ADDRESS })).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable-aws-account:foo"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Arrays.asList(new CharSequence[] { "droppable-http-remote-address:127.0.0.1" });

    UserHandler handler = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions).build();

    Job job = newJob(3);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "127.0.0.1");
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[0]);
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void norandomShedding() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withGlobalKey(null).withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions).build();

    Job job = newJob(3);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] {});
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void defaultShedding() throws Throwable {
    KeyGenerator kg = KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();

    Job job = newJob(3);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    try {
      handler.before(ctx);
      fail("should throw");
    } catch (ServiceUnavailableException e) {
    }

    List<String> expectedKeys = Arrays.asList("droppable");
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noShedOnKeyGeneratorFailure() throws Throwable {
    KeyGenerator kg = new KeyGenerator() {
      @Override
      public Iterable<CharSequence> getKeys(HandlerContext ctx) {
        throw new RuntimeException("bad KeyGenerator");
      }
    };
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler = newLoadShedHandlerBuilder(kg, capacity, strategy, exemptions).build();

    Job job = newJob(3);
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] {});
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noDuplicateThrottlerCalls() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    KeyGenerator kg2 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable"));
    ThrottlerMemoizer mem = new ThrottlerMemoizer(strategy);
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler1 = new LoadShedHandler.Builder()
      .withKeyGenerator(kg1)
      .withCapacity(capacity)
      .withThrottlerRetriever(mem)
      .withExemptions(exemptions)
      .build();
    UserHandler handler2 = new LoadShedHandler.Builder()
      .withKeyGenerator(kg2)
      .withCapacity(capacity)
      .withThrottlerRetriever(mem)
      //.withExemptions(exemptions) // was never used with global PREFIX
      .build();

    HandlerContext ctx;

    Job job1 = newJob(1);
    Identity identity1 = IdentityHelper.getIdentity(job1);
    identity1.setAttribute(Identity.AWS_ACCOUNT, "foo");

    ctx = com.amazon.coral.avail.Helper.newHandlerContext(job1);
    handler1.before(ctx);
    handler2.before(ctx);

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add("droppable-aws-account:foo");
    expectedKeys.add("droppable");
    assertEquals(expectedKeys, strategy.getSeenKeys());

    handler1.before(ctx);
    handler2.before(ctx);
    assertEquals(expectedKeys, strategy.getSeenKeys());

    Job job2 = newJob(1);
    ctx = com.amazon.coral.avail.Helper.newHandlerContext(job2);
    Identity identity2 = IdentityHelper.getIdentity(job2);
    identity2.setAttribute(Identity.AWS_ACCOUNT, "foo");
    expectedKeys.add("droppable-aws-account:foo");
    expectedKeys.add("droppable");

    handler1.before(ctx);
    handler2.before(ctx);
    assertEquals(expectedKeys, strategy.getSeenKeys());

    Job job3 = newJob(1);
    ctx = com.amazon.coral.avail.Helper.newHandlerContext(job3);
    Identity identity3 = IdentityHelper.getIdentity(job3);
    identity3.setAttribute(Identity.AWS_ACCOUNT, "bar");
    expectedKeys.add("droppable-aws-account:bar");
    expectedKeys.add("droppable");

    handler1.before(ctx);
    handler2.before(ctx);
    assertEquals(expectedKeys, strategy.getSeenKeys());

    handler1.before(ctx);
    handler2.before(ctx);
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void yesDuplicateThrottlerCalls() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    KeyGenerator kg2 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity();
    TrackedThrottler tt = new TrackedThrottler(newBlacklistThrottler("droppable"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    UserHandler handler1 = new LoadShedHandler.Builder()
      .withKeyGenerator(kg1)
      .withCapacity(capacity)
      .withThrottlerUnmemoized(tt)
      .withExemptions(exemptions)
      .build();
    UserHandler handler2 = new LoadShedHandler.Builder()
      .withKeyGenerator(kg2)
      .withCapacity(capacity)
      .withThrottlerUnmemoized(tt)
      //.withExemptions(exemptions) // was never used with global PREFIX
      .build();
    LoadShedHandler.Builder bldr = new LoadShedHandler.Builder();
    bldr.setKeyGenerator(kg2);
    bldr.setCapacity(capacity);
    bldr.setThrottlerUnmemoized(tt);
    UserHandler handler3 = bldr.build();

    Metrics metrics = new NullMetricsFactory().newMetrics();
    ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
    Identity identity1 = new Identity();
    identity1.setAttribute(Identity.AWS_ACCOUNT, "foo");

    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.when(ctx.getEstimatedInflightRequests()).thenReturn(new Integer(1));
    Mockito.when(ctx.getMetrics()).thenReturn(metrics);
    Mockito.when(ctx.getIdentity()).thenReturn(identity1);
    Mockito.when(ctx.getServiceIdentity()).thenReturn(sid);

    List<String> expectedKeys = new LinkedList<String>();

    expectedKeys.add("droppable-aws-account:foo");
    expectedKeys.add("droppable");
    handler1.before(ctx);
    assertEquals(expectedKeys, tt.getSeenKeys());

    expectedKeys.add("droppable-aws-account:foo");
    expectedKeys.add("droppable");
    handler2.before(ctx);
    assertEquals(expectedKeys, tt.getSeenKeys());

    expectedKeys.add("droppable-aws-account:foo");
    expectedKeys.add("droppable");
    handler3.before(ctx);
    assertEquals(expectedKeys, tt.getSeenKeys());
  }

  @Test
  public void correctMetricsForShedding() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable"));
    int requestCount = 3;
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    LoadShedHandler.Builder bldr = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions);
    UserHandler handler = bldr.build();

    TrackedMetrics metrics = new TrackedMetrics();
    ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "bar");

    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.when(ctx.getEstimatedInflightRequests()).thenReturn(new Integer(requestCount));
    Mockito.when(ctx.getMetrics()).thenReturn(metrics);
    Mockito.when(ctx.getIdentity()).thenReturn(identity);
    Mockito.when(ctx.getServiceIdentity()).thenReturn(sid);

    try {
      handler.before(ctx);
      fail("should throw");
    } catch (ServiceUnavailableException e) {
    }

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:bar", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(1L, metrics.getShedCount());
    assertEquals("droppable", metrics.getShedKey());
  }

  @Test
  public void correctMetricsForNoShedding() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable"));
    int requestCount = 1;
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    LoadShedHandler.Builder bldr = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions);
    UserHandler handler = bldr.build();

    TrackedMetrics metrics = new TrackedMetrics();
    ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "bar");

    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.when(ctx.getEstimatedInflightRequests()).thenReturn(new Integer(requestCount));
    Mockito.when(ctx.getMetrics()).thenReturn(metrics);
    Mockito.when(ctx.getIdentity()).thenReturn(identity);
    Mockito.when(ctx.getServiceIdentity()).thenReturn(sid);

    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:bar", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0L, metrics.getShedCount());
    assertEquals(null, metrics.getShedKey());
  }

  @Test
  public void correctMetricsForNoShedding_UnknownRequestCount() throws Throwable {
    KeyGenerator kg1 = KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentity(Identity.AWS_ACCOUNT).buildForIdentity();
    TrackedThrottler strategy = new TrackedThrottler(newBlacklistThrottler("droppable"));
    int requestCount = 1;
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    LoadShedHandler.Builder bldr = newLoadShedHandlerBuilder(kg1, capacity, strategy, exemptions);
    UserHandler handler = bldr.build();

    TrackedMetrics metrics = new TrackedMetrics();
    ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "bar");

    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.when(ctx.getEstimatedInflightRequests()).thenReturn(null); // unknown RequestCount
    Mockito.when(ctx.getMetrics()).thenReturn(metrics);
    Mockito.when(ctx.getIdentity()).thenReturn(identity);
    Mockito.when(ctx.getServiceIdentity()).thenReturn(sid);

    handler.before(ctx);

    List<String> expectedKeys = Arrays.asList(new String[] {});
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0L, metrics.getShedCount());
    assertEquals(null, metrics.getShedKey());
  }

  @Test
  public void builderSetters() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();

    assertNull(b.keygen);
    b.setKeyGenerator(Mockito.mock(KeyGenerator.class));
    assertNotNull(b.keygen);

    assertNull(b.retriever);
    Throttler s = new Throttler() {
      public boolean isThrottled(CharSequence key, Metrics m) {return false;}
    };
    b.setThrottlerUnmemoized(s);
    assertNotNull(b.retriever);

    java.util.Collection<CharSequence> el = new java.util.ArrayList<CharSequence>();
    assertNotNull(b.exemptions);
    assertEquals(el, b.exemptions);
    assertNotSame(el, b.exemptions);
    b.setExemptions(el);
    assertEquals(el, b.exemptions);
    assertSame(el, b.exemptions);

    assertNull(b.capacity);
    b.setCapacity(42);
    assertEquals(Integer.valueOf(42), b.capacity);
  }

  @Test
  public void builderWithers() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    LoadShedHandler.Builder r = null;
    assertNotSame(b, r);

    assertNull(b.keygen);
    r = b.withKeyGenerator(Mockito.mock(KeyGenerator.class));
    assertSame(b, r);
    assertNotNull(b.keygen);

    assertNull(b.retriever);
    Throttler t = new Throttler() {
      public boolean isThrottled(CharSequence key, Metrics m) {return false;}
    };
    r = b.withThrottler(t);
    assertSame(b, r);
    assertNotNull(b.retriever);

    java.util.Collection<CharSequence> el = new java.util.ArrayList<CharSequence>();
    assertNotNull(b.exemptions);
    assertEquals(el, b.exemptions);
    assertNotSame(el, b.exemptions);
    r = b.withExemptions(el);
    assertSame(b, r);
    assertEquals(el, b.exemptions);
    assertSame(el, b.exemptions);

    assertNull(b.capacity);
    r = b.withCapacity(42);
    assertSame(b, r);
    assertEquals(Integer.valueOf(42), b.capacity);
  }

  @Test
  public void builder() {
    LoadShedHandler h;
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setThrottlerUnmemoized(newBlacklistThrottler());
    b.setCapacity(123);

    b.setKeyGenerator(KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity());
    h = b.build();
    //assertEquals(IdentityLoadShedHandler.class, h.getClass());

    b.setKeyGenerator(KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentityOperation());
    h = b.build();
    //assertEquals(IdentityOperationLoadShedHandler.class, h.getClass());

    b.setKeyGenerator(KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForOperationIdentity());
    h = b.build();
    //assertEquals(OperationIdentityLoadShedHandler.class, h.getClass());

    b.setKeyGenerator(KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForOperation());
    h = b.build();
    //assertEquals(OperationLoadShedHandler.class, h.getClass());
  }

  @Test
  public void builderSansCapacity() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setThrottlerUnmemoized(newBlacklistThrottler());
    b.setKeyGenerator(KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity());
    try {
      b.build();
      Assert.fail("should throw");
    } catch (RuntimeException e) {}
  }

  @Test
  public void builderSansType() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setThrottlerUnmemoized(newBlacklistThrottler());
    b.setCapacity(123);
    try {
      b.build();
      Assert.fail("should throw");
    } catch (RuntimeException e) {}
  }

  @Test
  public void builderSansThrottler() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setKeyGenerator(KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity());
    b.setCapacity(123);
    try {
      b.build();
      Assert.fail("should throw");
    } catch (RuntimeException e) {}
  }

  private Throttler newBlacklistThrottler(CharSequence... keys) {
    return StaticThrottler.newBlacklist(Arrays.asList(keys));
  }
}
