// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import java.util.Set;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Collections;

import com.amazon.coral.service.*;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.model.NullModelIndex;

import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

public class IdentityKeyGeneratorTest {
  @Test(expected=RuntimeException.class)
  public void nullArg() {
    new IdentityKeyGenerator(null);
  }
  @Test(expected=RuntimeException.class)
  public void emptyList() {
    new IdentityKeyGenerator(Collections.<String>emptyList());
  }
  @Test(expected=RuntimeException.class)
  public void nullListElement() {
    new IdentityKeyGenerator(Arrays.asList("foo", null, "bar"));
  }
  @Test
  public void simple() {
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    identity.setAttribute(Identity.AWS_ACCESS_KEY, "123");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "127.0.0.1");
    // We shouldn't see these next two keys
    identity.setAttribute(Identity.IDS_CUSTOMER, "bar");
    identity.setAttribute("quux", "mumble");
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.doReturn(identity).when(ctx).getIdentity();

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("aws-account:foo");
    expected.add("aws-access-key:123");
    expected.add("http-remote-address:127.0.0.1");

    IdentityKeyGenerator kg = new IdentityKeyGenerator();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : kg.getKeys(ctx)) {
      actual.add(key.toString());
    }

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void missing() {
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "foo");
    // We shouldn't see these next two keys
    identity.setAttribute(Identity.IDS_CUSTOMER, "bar");
    identity.setAttribute("quux", "mumble");
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.doReturn(identity).when(ctx).getIdentity();

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("aws-account:foo");

    IdentityKeyGenerator kg = new IdentityKeyGenerator();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : kg.getKeys(ctx)) {
      actual.add(key.toString());
    }

    Assert.assertEquals(expected, actual);
  }
}
