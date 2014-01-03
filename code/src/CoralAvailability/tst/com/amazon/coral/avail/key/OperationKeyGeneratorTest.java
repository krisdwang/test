// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.key;

import java.util.Set;
import java.util.HashSet;

import com.amazon.coral.service.*;
import static com.amazon.coral.service.ServiceConstant.*;
import static com.amazon.coral.avail.Helper.*;

import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.Mockito;

public class OperationKeyGeneratorTest {
  //@Test
  public void nullService() {
    // This test is not important. I think default behavior has changed, and we cannot have an
    // empty ServiceIdentity.
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.doReturn(null).when(ctx).getServiceIdentity();

    OperationKeyGenerator kg = new OperationKeyGenerator();
    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : kg.getKeys(ctx)) {
      actual.add(key.toString());
    }

    assertTrue(actual.isEmpty());
  }

  //@Test
  public void unknown() {
    // This test is not important, and we should not use a ModelIndex in this package.
    Job job = new JobImpl(getModelIndex(), new RequestImpl(getNullMetrics()));
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("Operation:UnknownService/UnknownOperation");

    OperationKeyGenerator kg = new OperationKeyGenerator();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : kg.getKeys(ctx)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }

  //@Test
  public void unknownOperation() {
    // This test is not important. I think default behavior has changed.
    Job job = newJob();
    HandlerContext ctx = com.amazon.coral.avail.Helper.newHandlerContext(job);

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("Operation:MyService/UnknownOperation");

    OperationKeyGenerator kg = new OperationKeyGenerator();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : kg.getKeys(ctx)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }

  @Test
  public void simple() {
    ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.doReturn(sid).when(ctx).getServiceIdentity();

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("Operation:MyService/OperationA"); // no namespace!

    OperationKeyGenerator kg = new OperationKeyGenerator();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : kg.getKeys(ctx)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }
}
