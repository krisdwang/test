// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.measure.unit.Unit;

import com.amazon.coral.metrics.*;
import com.amazon.coral.service.*;
import com.amazon.coral.model.NullModelIndex;
import static com.amazon.coral.availability.Helper.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class LoadShedHandlerTest {

  @Test(expected=RuntimeException.class)
  public void nullBuilder() {
    KeyBuilder builder = null;
    ThrottlingStrategy strategy = newBlacklist();
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();
    new LoadShedHandler(builder, 0, strategy, exemptions);
  }

  @Test(expected=RuntimeException.class)
  public void nullStrategy() {
    KeyBuilder builder = new CrossProductKeyBuilder(Collections.<KeyBuilder>emptyList(), ",");
    ThrottlingStrategy strategy = null;
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.emptyList();
    new LoadShedHandler(builder, 0, strategy, exemptions);
  }

  @Test(expected=RuntimeException.class)
  public void negativeCapacity() {
    KeyBuilder builder = new CrossProductKeyBuilder(Collections.<KeyBuilder>emptyList(), ",");
    ThrottlingStrategy strategy = newBlacklist();
    int capacity = -1;
    Collection<CharSequence> exemptions = Collections.emptyList();
    new LoadShedHandler(builder, capacity, strategy, exemptions);
  }

  @Test(expected=RuntimeException.class)
  public void nullExemptions() {
    KeyBuilder builder = new CrossProductKeyBuilder(Collections.<KeyBuilder>emptyList(), ",");
    ThrottlingStrategy strategy = newBlacklist();
    int capacity = 0;
    Collection<CharSequence> exemptions = null;
    new LoadShedHandler(builder, capacity, strategy, exemptions);
  }

  @Test
  public void defaultKeyIsChecked() throws Throwable {
    KeyBuilder builder = new ListKeyBuilder(new CharSequence[0]);
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(0);
    handler.before(job);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable" });

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void keysAreProperyPrefixed() throws Throwable {
    KeyBuilder builder = new ListKeyBuilder("foo", "bar");
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    int capacity = 0;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(0);
    handler.before(job);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-foo", "droppable-bar", "droppable" });

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noSheddingUnderCapacity() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(1);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");

    handler.before(job);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:foo", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noSheddingAtCapacity() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(2);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");

    handler.before(job);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:foo", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noSheddingWithoutThrottlingAtCapacity() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(3);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");

    handler.before(job);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:foo", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void sheddingWithThrottlingAtCapacity() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy(newBlacklist("droppable-aws-account:foo"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(3);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");

    ServiceUnavailableException lse = null;
    try {
      handler.before(job);
    } catch (ServiceUnavailableException e) {
      lse = e;
    }
    assertNotNull(lse);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:foo" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noSheddingWithoutRequestCount() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy(newBlacklist("droppable-aws-account:foo"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(0);
    RequestCountHelper.setCount(job, null);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");

    handler.before(job);

    // The throttling checks are short-circuited if the request count isn't available.
    List<String> expectedKeys = Arrays.asList(new String[] {});
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void keyBuilderFailure() throws Throwable {
    KeyBuilder builder = new KeyBuilder() {
      @Override
      public Iterable<CharSequence> getKeys(Job job) {
        throw new RuntimeException();
      }
    };
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(1);

    handler.before(job);
  }

  @Test
  public void strategyFailure() throws Throwable {
    KeyBuilder builder = new ListKeyBuilder(new CharSequence[0]);
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        throw new RuntimeException();
      }
    };
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(1);

    handler.before(job);
  }

  @Test
  public void noExemptShedding() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT, Identity.HTTP_REMOTE_ADDRESS }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy(newBlacklist("droppable-aws-account:foo"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Arrays.asList(new CharSequence[] { "droppable-http-remote-address:127.0.0.1" });

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(3);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "127.0.0.1");

    handler.before(job);

    List<String> expectedKeys = Arrays.asList(new String[0]);
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void defaultShedding() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy(newBlacklist("droppable"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job = newJob(3);

    ServiceUnavailableException lse = null;
    try {
      handler.before(job);
    } catch (ServiceUnavailableException e) {
      lse = e;
    }
    assertNotNull(lse);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void noDuplicateStrategyCalls() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy(newBlacklist("droppable"));
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    Job job1 = newJob(1);
    Identity identity1 = IdentityHelper.getIdentity(job1);
    identity1.setAttribute(Identity.AWS_ACCOUNT, "foo");

    handler.before(job1);

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add("droppable-aws-account:foo");
    expectedKeys.add("droppable");
    assertEquals(expectedKeys, strategy.getSeenKeys());

    handler.before(job1);
    assertEquals(expectedKeys, strategy.getSeenKeys());

    Job job2 = newJob(1);
    Identity identity2 = IdentityHelper.getIdentity(job2);
    identity2.setAttribute(Identity.AWS_ACCOUNT, "foo");
    expectedKeys.add("droppable-aws-account:foo");
    expectedKeys.add("droppable");

    handler.before(job2);
    assertEquals(expectedKeys, strategy.getSeenKeys());

    Job job3 = newJob(1);
    Identity identity3 = IdentityHelper.getIdentity(job3);
    identity3.setAttribute(Identity.AWS_ACCOUNT, "bar");
    expectedKeys.add("droppable-aws-account:bar");
    expectedKeys.add("droppable");

    handler.before(job3);
    assertEquals(expectedKeys, strategy.getSeenKeys());

    handler.before(job3);
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void correctMetricsForShedding() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy(newBlacklist("droppable"));
    int requestCount = 3;
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    TrackedMetrics metrics = new TrackedMetrics();
    Job job = newJob(metrics, requestCount);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "bar");

    ServiceUnavailableException lse = null;
    try {
      handler.before(job);
    } catch (ServiceUnavailableException e) {
      lse = e;
    }
    assertNotNull(lse);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:bar", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(1L, metrics.getShedCount());
    assertEquals("droppable", metrics.getShedKey());
  }

  @Test
  public void correctMetricsForNoShedding() throws Throwable {
    KeyBuilder builder = new IdentityKeyBuilder(Arrays.asList(new String[] { Identity.AWS_ACCOUNT }));
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy(newBlacklist("droppable"));
    int requestCount = 1;
    int capacity = 2;
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    Handler handler = new LoadShedHandler(builder, capacity, strategy, exemptions);

    TrackedMetrics metrics = new TrackedMetrics();
    Job job = newJob(metrics, requestCount);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "bar");

    handler.before(job);

    List<String> expectedKeys = Arrays.asList(new String[] { "droppable-aws-account:bar", "droppable" });
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0L, metrics.getShedCount());
    assertEquals(null, metrics.getShedKey());
  }

  @Test
  public void builderSetters() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();

    assertNull(b.keys);
    b.setKeys(new java.util.ArrayList<String>());
    assertNotNull(b.keys);

    assertNull(b.strat);
    ThrottlingStrategy s = new ThrottlingStrategy() {
      public boolean isThrottled(CharSequence key) {return false;}
    };
    b.setStrategy(s);
    assertNotNull(b.strat);

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

    assertNull(b.keys);
    r = b.withKeys(new java.util.ArrayList<String>());
    assertSame(b, r);
    assertNotNull(b.keys);

    assertNull(b.strat);
    ThrottlingStrategy s = new ThrottlingStrategy() {
      public boolean isThrottled(CharSequence key) {return false;}
    };
    r = b.withStrategy(s);
    assertSame(b, r);
    assertNotNull(b.strat);

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

    assertNull(b.cls);
    r = b.withType(LoadShedHandler.Type.IDENTITY);
    assertSame(b, r);
    assertEquals(LoadShedHandler.Type.IDENTITY, b.cls);
  }

  @Test
  public void builder() {
    LoadShedHandler h;
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setStrategy(newBlacklist());
    b.setCapacity(123);

    b.setType(LoadShedHandler.Type.IDENTITY);
    h = b.build();
    assertEquals(IdentityLoadShedHandler.class, h.getClass());

    b.setType(LoadShedHandler.Type.IDENTITY_OPERATION);
    h = b.build();
    assertEquals(IdentityOperationLoadShedHandler.class, h.getClass());

    b.setType(LoadShedHandler.Type.OPERATION_IDENTITY);
    h = b.build();
    assertEquals(OperationIdentityLoadShedHandler.class, h.getClass());

    b.setType(LoadShedHandler.Type.OPERATION);
    h = b.build();
    assertEquals(OperationLoadShedHandler.class, h.getClass());
  }

  @Test(expected=RuntimeException.class)
  public void builderSansCapacity() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setStrategy(newBlacklist());
    b.setType(LoadShedHandler.Type.IDENTITY);
    b.build();
  }

  @Test(expected=RuntimeException.class)
  public void builderSansType() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setStrategy(newBlacklist());
    b.setCapacity(123);
    b.build();
  }

  @Test(expected=RuntimeException.class)
  public void builderSansStrategy() {
    LoadShedHandler.Builder b = LoadShedHandler.newBuilder();
    b.setType(LoadShedHandler.Type.IDENTITY);
    b.setCapacity(123);
    b.build();
  }

  private ThrottlingStrategy newBlacklist(CharSequence... keys) {
    return StaticThrottlingStrategy.newBlacklist(Arrays.asList(keys));
  }
}
