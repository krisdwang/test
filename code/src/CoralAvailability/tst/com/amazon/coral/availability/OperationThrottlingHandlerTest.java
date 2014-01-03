package com.amazon.coral.availability;

import java.util.Arrays;
import java.util.List;

import com.amazon.coral.service.*;
import static com.amazon.coral.service.ServiceConstant.*;
import static com.amazon.coral.availability.Helper.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class OperationThrottlingHandlerTest {
  @Test
  public void correctKeys() throws Throwable {
    Job job = newJob(1);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = Arrays.asList(new String[] {
      "Operation:MyService/OperationA", ""
    });

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new OperationThrottlingHandler(strategy);

    h.before(job);

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }

  @Test
  public void prefix() throws Throwable {
    Job job = newJob(1);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = Arrays.asList(new String[] {
      "OP:Operation:MyService/OperationA", ""
    });

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new OperationThrottlingHandler(true, strategy);

    h.before(job);

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }
}
