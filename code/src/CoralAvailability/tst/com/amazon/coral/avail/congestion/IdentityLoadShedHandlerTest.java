// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.congestion;

import java.util.Arrays;
import java.util.Collections;

import com.amazon.coral.metrics.*;
import com.amazon.coral.service.*;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.avail.key.*;



import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

public class IdentityLoadShedHandlerTest {
  @Test
  public void migrationBuilder() throws Throwable {
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "not-throttled");
    Metrics metrics = new NullMetricsFactory().newMetrics();

    Throttler throttler = Mockito.mock(Throttler.class);
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.when(ctx.getEstimatedInflightRequests()).thenReturn(new Integer(1));
    Mockito.when(ctx.getMetrics()).thenReturn(metrics);
    Mockito.when(ctx.getIdentity()).thenReturn(identity);

    LoadShedHandler h = LoadShedHandler.buildForIdentity(
        Arrays.asList(new String[]{Identity.AWS_ACCOUNT}),
        1,
        throttler,
        Collections.<CharSequence>emptyList());
    h.before(ctx);

    com.amazon.coral.avail.Helper.verifyKeys(throttler, "droppable-aws-account:throttled", "droppable");
  }
}
