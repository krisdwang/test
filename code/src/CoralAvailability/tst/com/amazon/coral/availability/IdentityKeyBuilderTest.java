package com.amazon.coral.availability;

import java.util.Set;
import java.util.HashSet;

import com.amazon.coral.service.*;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.model.NullModelIndex;

import org.junit.Test;
import static org.junit.Assert.*;

public class IdentityKeyBuilderTest {
  @Test
  public void simple() {
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(new NullMetricsFactory().newMetrics()));

    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    identity.setAttribute(Identity.AWS_ACCESS_KEY, "123");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "127.0.0.1");
    // We shouldn't see these next two keys
    identity.setAttribute(Identity.IDS_CUSTOMER, "bar");
    identity.setAttribute("quux", "mumble");

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("aws-account:foo");
    expected.add("aws-access-key:123");
    expected.add("http-remote-address:127.0.0.1");

    IdentityKeyBuilder builder = new IdentityKeyBuilder();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }

  @Test
  public void missing() {
    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(new NullMetricsFactory().newMetrics()));

    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    // We shouldn't see these next two keys
    identity.setAttribute(Identity.IDS_CUSTOMER, "bar");
    identity.setAttribute("quux", "mumble");

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("aws-account:foo");

    IdentityKeyBuilder builder = new IdentityKeyBuilder();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }
}
