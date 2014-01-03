package com.amazon.coral.availability;

import java.util.Arrays;
import java.util.List;

import com.amazon.coral.metrics.*;
import com.amazon.coral.service.*;
import com.amazon.coral.model.NullModelIndex;
import static com.amazon.coral.service.ServiceConstant.*;
import static com.amazon.coral.availability.Helper.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class OperationLoadShedHandlerTest {
  @Test
  public void correctKeys() throws Throwable {
    Job job = newJob(1);
    job.setAttribute(SERVICE_OPERATION_MODEL, getOperation("OperationA"));
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = Arrays.asList(new String[] {
      "droppable-Operation:MyService/OperationA", "droppable"
    });

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    LoadShedHandler h = new OperationLoadShedHandler(1, strategy);

    h.before(job);

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }
}
