package com.amazon.coral.availability;

import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;

import com.amazon.coral.service.*;
import static com.amazon.coral.service.ServiceConstant.*;
import com.amazon.coral.model.*;
import static com.amazon.coral.availability.Helper.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class IdentityOperationThrottlingHandlerTest {

  @Test(expected=RuntimeException.class)
  public void nullStrategy() {
    new IdentityOperationThrottlingHandler(null);
  }

  @Test
  public void noIdentityToThrottleOn() {
    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy);

    TrackedMetrics m = new TrackedMetrics();
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(m));
    h.before(job);

    // Check if the DEFAULT was tried
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0, m.getThrottleCount());
  }

  @Test
  public void identityToThrottleOn() {
    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy);

    TrackedMetrics m = new TrackedMetrics();
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(m));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    h.before(job);

    // Check if the DEFAULT was tried
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0, m.getThrottleCount());
  }

  @Test
  public void operationToThrottleOn() {
    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy);

    TrackedMetrics m = new TrackedMetrics();
    Job job = newJob(m);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));

    h.before(job);

    // Check if the DEFAULT was tried
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0, m.getThrottleCount());
  }

  @Test
  public void operationAndIdentityToThrottleOn() {
    TrackedMetrics m = new TrackedMetrics();
    Job job = newJob(m);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add(Identity.AWS_ACCOUNT+":throttled,Operation:MyService/OperationA");
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy);

    h.before(job);

    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0, m.getThrottleCount());
  }

  @Test
  public void badStrategyShouldNotCauseFailure() throws Throwable {
    TrackedMetrics m = new TrackedMetrics();
    Job job = newJob(m);

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        super.isThrottled(key);
        throw new UnsupportedOperationException();
      }
    };
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy);

    h.before(job);

    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0, m.getThrottleCount());
  }

  @Test
  public void defaultStrategyCorrectness() throws Throwable {
    TrackedMetrics m = new TrackedMetrics();
    Job job = newJob(m);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add(Identity.AWS_ACCOUNT+":throttled,Operation:MyService/OperationA");
    expectedKeys.add("");

    final int[] throttleCount = new int[] { 0 };
    final String[] throttledKey = new String[] { null };

    @SuppressWarnings("serial")
    ThrottlingStrategy strategy = new LocalThrottlingStrategy(
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
    };
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy);

    try {
      h.before(job);
    } catch(ThrottlingException t) {}

    assertEquals(1, throttleCount[0]);
    assertEquals("", throttledKey[0]);
    assertEquals(1, m.getThrottleCount());
  }

  @Test
  public void nonDefaultStrategyCorrectness() throws Throwable {
    TrackedMetrics m = new TrackedMetrics();
    Job job = newJob(m);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = new LinkedList<String>();
    final String throttleKey = Identity.AWS_ACCOUNT+":throttled,Operation:MyService/OperationA";
    expectedKeys.add(throttleKey);

    final int[] throttleCount = new int[] { 0 };
    final String[] throttledKey = new String[] { null };

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        super.isThrottled(key);
        if (throttleKey.contentEquals(key)) {
          ++throttleCount[0];
          throttledKey[0] = key.toString();
          return true;
        }
        return false;
      }
    };
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy);

    try {
      h.before(job);
    } catch(ThrottlingException t) {}

    assertEquals(1, throttleCount[0]);
    assertEquals(throttleKey, throttledKey[0]);
    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(1, m.getThrottleCount());
  }

  //@Test // this now warns instead
  public void noDefaultExemptions() {
    final int[] exempt = new int[] { 0 };

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();

    // A handler that considers any identity exempt
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy) {
      @Override
      public boolean isExempt(CharSequence key) {
        exempt[0]++;
        return true;
      }
    };

    TrackedMetrics m = new TrackedMetrics();
    Job job = newJob(m);

    List<String> expectedKeys = new LinkedList<String>();
    expectedKeys.add("");

    h.before(job);

    // Check that we did not exempt an id, and that we did engage the throttler
    assertEquals(0, exempt[0]);
    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void exemptions() {
    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();

    List<CharSequence> exemptions = new LinkedList<CharSequence>();
    exemptions.add(Identity.AWS_ACCOUNT+":throttled,Operation:MyService/OperationA");

    // A handler that considers any identity exempt
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy, exemptions);

    // Job w/ exempted id
    TrackedMetrics m = new TrackedMetrics();
    Job job = newJob(m);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = new LinkedList<String>();

    h.before(job);

    assertEquals(expectedKeys, strategy.getSeenKeys());
    assertEquals(0, m.getThrottleCount());
  }

  @Test
  public void ignoredKeys() {
    // Ensure only the "sane" identity keys are being used to throttle on
    Set<String> expectedKeys = new HashSet<String>();
    expectedKeys.add(Identity.AWS_ACCOUNT+":throttled,Operation:MyService/OperationA");
    expectedKeys.add(Identity.HTTP_REMOTE_ADDRESS+":throttled,Operation:MyService/OperationA");
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(strategy);

    Job job = newJob(new TrackedMetrics());
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
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
    expectedKeys.add(Identity.AWS_ACCOUNT+":throttled,Operation:MyService/OperationA");
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(Arrays.<String>asList(Identity.AWS_ACCOUNT), strategy);

    Job job = newJob(new TrackedMetrics());
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
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
    expectedKeys.add("ID_OP:" + Identity.AWS_ACCOUNT+":throttled,Operation:MyService/OperationA");
    expectedKeys.add("");

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new IdentityOperationThrottlingHandler(Arrays.<String>asList(Identity.AWS_ACCOUNT), true, strategy);

    Job job = newJob(new TrackedMetrics());
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "0.0.0.0");
    identity.setAttribute(Identity.HTTP_USER_AGENT, "Mosaic");
    identity.setAttribute(Identity.AWS_ACCESS_KEY, "foo");

    h.before(job);

    assertEquals(expectedKeys, new HashSet<String>(strategy.getSeenKeys()));
  }
}
