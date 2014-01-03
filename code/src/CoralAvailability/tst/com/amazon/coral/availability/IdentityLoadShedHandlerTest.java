package com.amazon.coral.availability;

import java.util.Arrays;
import java.util.List;
import javax.measure.unit.Unit;

import com.amazon.coral.metrics.*;
import com.amazon.coral.service.*;
import static com.amazon.coral.availability.Helper.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class IdentityLoadShedHandlerTest {
  @Test
  public void correctKeys() throws Throwable {
    Job job = newJob(1);
    Identity identity = IdentityHelper.getIdentity(job);
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    List<String> expectedKeys = Arrays.asList(new String[] {
      "droppable-" + Identity.AWS_ACCOUNT+":throttled", "droppable"
    });

    TrackedThrottlingStrategy strategy = new TrackedThrottlingStrategy();
    LoadShedHandler h = new IdentityLoadShedHandler(1, strategy);

    h.before(job);

    assertEquals(expectedKeys, strategy.getSeenKeys());
  }
}
