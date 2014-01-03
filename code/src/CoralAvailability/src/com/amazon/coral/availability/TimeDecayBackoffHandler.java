// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import com.amazon.coral.service.Handler;
import com.amazon.coral.service.Job;
import com.amazon.coral.service.TimeoutException;
import com.amazon.coral.metrics.Metrics;
import javax.measure.unit.Unit;

/**
 * A {@code TimeDecayBackoffHandler} is a throttling strategy that is
 * based on the simple idea that the average latency of service calls and/or the timeout
 * rate increases dramatically when the service is in trouble.
 *
 * This code is based on the original COLTStrategy implementation for the JCT:
 * //brazil/src/shared/platform/JavaServiceAvailabilityManager/release/javaServiceAvailabilityManager/amazon/platform/sam/strategies/COLTAvailabilityStrategy.java
 *
 * This explaination was taken from the wiki that explains the strategy:
 *
 * In order to detect latency increase, we maintain two time-decayed weighted averages of
 * individual latency data points. These two averages have different factors for decaying
 * old values. By tuning the decay factor appropriately, one of the averages determines the
 * long-term average latency, and the other one determines the short-term average latency.
 * In particular, in the long-term average, higher weight is given to older data points
 * compared to that in the short-term average.
 *
 * Given that we have these two values, we compute the percentage change in latency between
 * short-term and long-term. A positive change is an indication of latency increase over time.
 * We maintain a time-decayed weighted average of these (positive) percentage changes of latency.
 * This value is called the LatencyTroubleIndicator (LTI). The way to interpret LTI is that it
 * is the rate at which the latency is increasing consistently. We have a threshold for LTI
 * (default, 0.35) above which we stop making non-critical calls, and another threshold
 * (default, 0.7) above which we stop making all calls.
 *
 * Just as we do for latency, we also keep statistics for timeout rate. Whenever we get a
 * latency or timeout data point, we recalculate the current timeout rate by looking at the
 * fraction of calls that have timed out in the last n (default, 32) calls. We then calculate
  the short-term and long-term timeout rate, and a corresponding TimeoutTroubleIndicator (TTI).
 * We place thresholds on TTI for backing off from the service.
 *
 * Finally, both the LTI and TTI are useful in backing off when the service is getting into trouble.
 * However, once the service is in trouble for an extended period of time, the long-term statistics
 * become similar to short-term statistics, and therefore LTI and TTI do not give us useful information.
 * Therefore, we also back off from services when the short-term timeout rate is greater than a
 * certain threshold. The default value is 0.5 for non-critical calls and 0.8 for all calls.
 *
 * @author Ameet Vaswani <vaswani@amazon.com>
 * @author Eric Crahen <crahen@amazon.com>
 * @version 1.0
 * @deprecated Nobody is using this.
 */
@Deprecated
public class TimeDecayBackoffHandler implements Handler {

  private final TimeDecayHealth health = new TimeDecayHealth();
  /*
   * no need to mark the following "last" variable as volatile.
   * Because, its read and write operations always happen within a synchronized block.
   */
  private long last;

  @Override
  public void before(Job job) {

    boolean throttled = false;
    try {

      synchronized (this) {
        double status = health.getHealth();
        if(status == TimeDecayHealth.GreenHealth || status == TimeDecayHealth.YellowHealth)
          return;

        // A small sample of requests are allowed to pass
        // through to help test the status of the service
        long now = System.currentTimeMillis();
        if((now - last) >= getGuineaPigInterval()) {
          last = now;
          return;
        }
      }

      throttled = true;
      throw new BackOffException();

    } finally {
      Metrics metrics = job.getMetrics();
      metrics.addCount("Backoff:Critical", (throttled ? 1.0 : 0.0), Unit.ONE);
    }

  }

  @Override
  public void after(Job job) {

    // Calculate the perceived Latency
    long latency = Math.max(0, System.currentTimeMillis() - job.getCreation());
    // Check for a timeout
    boolean isTimeout = job.getFailure() instanceof TimeoutException;

    synchronized (this) {
      health.recordLatency((double)latency);
      if(isTimeout)
        health.recordTimeout();
    }

  }

  private long getGuineaPigInterval() {
    return 60 * 1000;
  }

}
