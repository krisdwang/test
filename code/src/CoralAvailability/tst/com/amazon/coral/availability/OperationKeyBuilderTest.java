package com.amazon.coral.availability;

import java.util.Set;
import java.util.HashSet;

import com.amazon.coral.service.*;
import static com.amazon.coral.service.ServiceConstant.*;
import static com.amazon.coral.availability.Helper.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class OperationKeyBuilderTest {
  @Test
  public void nullService() {
    Job job = new JobImpl(getNullModelIndex(), new RequestImpl(getNullMetrics()));

    OperationKeyBuilder builder = new OperationKeyBuilder();
    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertTrue(actual.isEmpty());
  }

  @Test
  public void unknown() {
    Job job = new JobImpl(getModelIndex(), new RequestImpl(getNullMetrics()));

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("Operation:UnknownService/UnknownOperation");

    OperationKeyBuilder builder = new OperationKeyBuilder();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }

  @Test
  public void unknownOperation() {
    Job job = newJob();

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("Operation:MyService/UnknownOperation");

    OperationKeyBuilder builder = new OperationKeyBuilder();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }

  @Test
  public void simple() {
    Job job = newJob();
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));

    Set<CharSequence> expected = new HashSet<CharSequence>();
    expected.add("Operation:MyService/OperationA");

    OperationKeyBuilder builder = new OperationKeyBuilder();

    Set<CharSequence> actual = new HashSet<CharSequence>();
    for (CharSequence key : builder.getKeys(job)) {
      actual.add(key.toString());
    }

    assertEquals(expected, actual);
  }
}
