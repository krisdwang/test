package com.amazon.coral.availability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.measure.unit.Unit;

import org.junit.Test;

import com.amazon.coral.metrics.BrokenMetrics;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.model.NullModelIndex;
import com.amazon.coral.service.Identity;
import com.amazon.coral.service.IdentityHelper;
import com.amazon.coral.service.Job;
import com.amazon.coral.service.JobImpl;
import com.amazon.coral.service.RequestImpl;

public class ShadowThrottlingHandlerTest {

  @Test
  public void testMetrics() {

    final double metric[] = new double[] {1, 0};
    final CharSequence prop[] = new CharSequence[] {null};
    final int strategy[] = new int[] {0};

    Metrics m = new BrokenMetrics() {
      @Override
      public void addCount(String name, double value, Unit<?> unit, int repeat) {
        metric[0] = value;
        assertEquals("WouldThrottle", name);
      }
      @Override
      public void addTime(String name, double value, Unit<?> unit, int repeat) {
        metric[1] = 1;
        assertEquals("WouldThrottleTime", name);
      }
      @Override
      public void addProperty(String name, CharSequence value) {
        prop[0] = value;
        assertEquals("WouldThrottleKey", name);
      }
    };

    ShadowThrottlingHandler h = new ShadowThrottlingHandler(new IdentityThrottlingHandler(new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        strategy[0]++;
        return true;
      }
    }));

    // Since our strategy is hard-coded above, the actual identity doesn't matter
    Identity identity = new Identity();
    identity.setAttribute(Identity.AWS_ACCOUNT, "throttled");

    Job job = new JobImpl(new NullModelIndex(), new RequestImpl(m));
    IdentityHelper.setIdentity(job, identity);
    try {
      h.before(job);
    } catch(ThrottlingException e) {
      fail();
    }

    // Check the metric emitted and how many times we consulted the strategy
    assertEquals(1.0d, metric[0], 0);
    assertEquals(1.0d, metric[1], 0);
    assertEquals(1, strategy[0]);
  }

}
