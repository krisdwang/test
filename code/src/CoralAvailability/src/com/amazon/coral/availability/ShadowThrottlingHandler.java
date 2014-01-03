// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.Constant;
import com.amazon.coral.service.Job;
import com.amazon.coral.throttle.api.Throttler;

/**
 * A {@code ThrottlingHandler} that only emits hypothetical metrics. If placed in the chain,
 * this handler has no effect on a job.
 * <p>
 * This is useful for comparing the effective performance of two different throttling providers.
 * <p>
 * Copyright (c) 2010 Amazon.com. All rights reserved.
 * @author Sam Young &lt;sayo@amazon.com&gt;
 * @Deprecated
 */
@Deprecated
public class ShadowThrottlingHandler extends ThrottlingHandler {

  private static final Log log = LogFactory.getLog(ShadowThrottlingHandler.class);
  private final static Constant<ThrottlingStrategy> SHADOW_MEMOIZER = new Constant<ThrottlingStrategy>(ThrottlingStrategy.class, "Secondary Throttling Memoizer");
  private final Throttler throttler;

  public ShadowThrottlingHandler(ThrottlingHandler handler) {
    super(handler.getBuilder(), handler.getThrottlerForShadow(), handler.getExemptions());
    this.throttler = handler.getThrottlerForShadow();
  }

  @Override
  public void before(Job job) {
    try {
      super.before(job);
    } catch (ThrottlingException e) {
      // We ignore the exception in shadow handlers.
    } catch (Throwable t) {
      // We ignore the exception in shadow handlers, but if
      // we reached this block, something is wrong so log a warning.
      log.warn("Error in shadowed throttling handler", t);
    }
  }

  @Override
  void emitMetrics(Job job, boolean throttled, long start, CharSequence throttledKey) {
    // Has to emit 0 or 1 for each option
    Metrics metrics = job.getMetrics();
    metrics.addCount("WouldThrottle", (throttled ? 1.0 : 0.0), Unit.ONE);
    // Provide timing information to capture how much time is spent on throttling
    metrics.addTime("WouldThrottleTime", Math.max(0,System.nanoTime()-start), SI.NANO(SI.SECOND));
    if (throttled)
      metrics.addProperty("WouldThrottleKey", throttledKey);
  }

  @Override
  protected Throttler getThrottler(Job job) {
    Throttler s = job.getAttribute(SHADOW_MEMOIZER);
    if(s == null) {
      s = new MemoizedThrottlingStrategy(this.throttler);
      job.setAttribute(SHADOW_MEMOIZER, s);
    }
    return s;
  }

}
