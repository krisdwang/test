// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import org.junit.Assert;

import javax.measure.unit.Unit;

import org.junit.Test;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.service.MyHandlerContext;

public class ThrottlerMemoizerTest {

  HandlerContext newHandlerContext() {
    /*
    Metrics metrics = new NullMetricsFactory().newMetrics();
    ServiceIdentity sid = com.amazon.coral.availability.Helper.fakeServiceIdentity("foo#", "MyService", "OperationA");
    Identity identity1 = new Identity();
    identity1.setAttribute(Identity.AWS_ACCOUNT, "foo");

    HandlerContext ctx = Mockito.mock(HandlerContext.class);
    Mockito.when(ctx.getEstimatedInflightRequests()).thenReturn(new Integer(1));
    Mockito.when(ctx.getMetrics()).thenReturn(metrics);
    Mockito.when(ctx.getIdentity()).thenReturn(identity1);
    Mockito.when(ctx.getServiceIdentity()).thenReturn(sid);
    */
    return MyHandlerContext.create(false);
  }

  @Test(expected=RuntimeException.class)
  public void nullThrottler() {
    new ThrottlerMemoizer(null);
  }

  @Test
  public void memoizedThrottler() {
    HandlerContext ctx = newHandlerContext();

    final int count[] = new int[]{0};

    Throttler s = new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics metrics) {
        count[0]++;
        return true;
      }
    };
    ThrottlerRetriever mem = new ThrottlerMemoizer(s);
    Assert.assertTrue(s != mem.getThrottler(ctx));
    s = mem.getThrottler(ctx);

    Assert.assertTrue( s.isThrottled("a", null) );
    Assert.assertTrue( s.isThrottled("a", null) );
    Assert.assertTrue( s.isThrottled("a", null) );
    Assert.assertEquals(count[0], 1);
  }

  @Test
  public void memoizedMetrics() {
    HandlerContext ctx = newHandlerContext();

    final Metrics[] r = new Metrics[1];

    Throttler t = new Throttler() {
      @Override
      public boolean isThrottled(CharSequence key, Metrics metrics) {
        r[0] = metrics;
        return false;
      }
    };
    ThrottlerRetriever mem = new ThrottlerMemoizer(t);
    Assert.assertEquals(MemoizedThrottler.class, mem.getThrottler(ctx).getClass());
    t = mem.getThrottler(ctx);

    Metrics m = new NullMetricsFactory().newMetrics();

    t.isThrottled("foo", m);
    Assert.assertEquals(m, r[0]);
  }

}
