package com.amazon.coral.availability;

import java.util.Arrays;
import java.util.List;

import com.amazon.coral.service.*;
import static com.amazon.coral.service.ServiceConstant.*;
import static com.amazon.coral.availability.Helper.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class OperationIdentityThrottlingHandlerTest {
  @Test
  public void correctKeys() throws Throwable {
    Job job = newJob(1);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = Arrays.asList(new String[] {
      "Operation:MyService/OperationA," + Identity.AWS_ACCOUNT+":throttled", ""
    });

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new OperationIdentityThrottlingHandler(strategy);

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
      "OP_ID:Operation:MyService/OperationA," + Identity.AWS_ACCOUNT+":throttled", ""
    });

    List<String> identityKeys = Arrays.asList(new String[] {
        Identity.AWS_ACCOUNT
      });

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    ThrottlingHandler h = new OperationIdentityThrottlingHandler(identityKeys, true, strategy);

    h.before(job);

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }
}
