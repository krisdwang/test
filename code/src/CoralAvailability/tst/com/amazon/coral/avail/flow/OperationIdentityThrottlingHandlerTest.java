// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.flow;

import java.util.Arrays;
import java.util.Collections;

import com.amazon.coral.metrics.*;
import com.amazon.coral.service.*;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.avail.key.*;



import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

public class OperationIdentityThrottlingHandlerTest {
  @Test
  public void migrationBuilder() throws com.amazon.coral.availability.ThrottlingException {
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "not-throttled");
    ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
    Metrics metrics = new NullMetricsFactory().newMetrics();

    Throttler throttler = Mockito.mock(Throttler.class);
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.when(ctx.getMetrics()).thenReturn(metrics);
    Mockito.when(ctx.getIdentity()).thenReturn(identity);
    Mockito.when(ctx.getServiceIdentity()).thenReturn(sid);

    ThrottlingHandler h = ThrottlingHandler.buildForOperationIdentity(
        Arrays.asList(new String[]{Identity.AWS_ACCOUNT}),
        true,
        throttler,
        Collections.<CharSequence>emptyList());
    h.before(ctx);

    com.amazon.coral.avail.Helper.verifyKeys(throttler, "OP_ID:Operation:MyService/OperationA,aws-account:throttled", "");
  }
  @Test
  public void migrationBuilderSansPrefix() throws com.amazon.coral.availability.ThrottlingException {
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");
    identity.setAttribute(Identity.HTTP_REMOTE_ADDRESS, "not-throttled");
    ServiceIdentity sid = com.amazon.coral.avail.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
    Metrics metrics = new NullMetricsFactory().newMetrics();

    Throttler throttler = Mockito.mock(Throttler.class);
    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.when(ctx.getMetrics()).thenReturn(metrics);
    Mockito.when(ctx.getIdentity()).thenReturn(identity);
    Mockito.when(ctx.getServiceIdentity()).thenReturn(sid);

    ThrottlingHandler h = ThrottlingHandler.buildForOperationIdentity(
        Arrays.asList(new String[]{Identity.AWS_ACCOUNT}),
        false, // no prefix
        throttler,
        Collections.<CharSequence>emptyList());
    h.before(ctx);

    com.amazon.coral.avail.Helper.verifyKeys(throttler, "Operation:MyService/OperationA,aws-account:throttled", "");
  }
}
