// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import javax.measure.unit.SI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.annotation.HttpError;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.service.HttpWireProtocol;
import com.amazon.coral.service.UserHandler;

/**
 * A {@code NeutronHandler} (as in Neutron Star) is a {@code UserHandler} that will prevent Load Balancer blackholes by
 * delaying error responses when the error rate exceeds the specified threshold.
 *
 * A response is considered an error when the HTTP Response code defined on the generated exception is in the 5xx range.
 * For model generated exceptions, be sure to include the 'fault' trait, see: {@link https
 * //w.amazon.com/index.php/Coral/Model/XML/Traits/Fault}.
 *
 * There are two configurable parameters: allowedPerSecond is the number of errors allowed per second before the
 * automatic delay is added to the error response. delay is the maximum number of milliseconds to delay an error
 * response.
 *
 * Note that the allowedPerSecond is an estimated rate. It will not be 100% exact, but will be close enough for the
 * given purpose. Likewise, the delay is an estimated amount because the granularity of the system clock will not assure
 * 100% accuracy. This also is close enough for the given purpose.
 *
 * Add this handler before the HttpHandler, and as close to the top of the handler chain as possible. It may be
 * desirable to have it after the Log4jAwareRequestIdHandler and/or Wire Logging handlers if they are present.
 */
/*
 * Here is a sample configuration:
 *  <code>
 *    <bean class="com.amazon.coral.availability.NeutronHandler">
 *      <constructor-arg>
 *        <bean class="com.amazon.coral.availability.NeutronHandler$Config">
 *          <property name="delay" value="20"/>
 *          <property name="allowedPerSecond" value="4"/>
 *        </bean>
 *      </constructor-arg>
 *    </bean>
 *  </code>
 * @Deprecated prefer com.amazon.coral.avail.flow.NeutronHandler
 */
@Deprecated
public class NeutronHandler extends UserHandler {
  private final static Log log = LogFactory.getLog(NeutronHandler.class);

  private final long delay;;
  private final int allowedPerSecond;
  private final Object lock = new Object();
  private volatile double failureRate = 0.0;
  private volatile long last = currentTimeMillis();

  public NeutronHandler(Config config) {
    if (config == null) {
      throw new IllegalArgumentException("NeutronHandler Config must be provided.");
    }

    delay = config.getDelay();
    if (delay <= 0) {
      throw new IllegalArgumentException("Delay must be positive.");
    }

    allowedPerSecond = config.getAllowedPerSecond();
    if (allowedPerSecond < 0) {
      throw new IllegalArgumentException("AllowedPerSecond must not be negative.");
    }
  }

  @Override
  public void after(HandlerContext context) throws Exception {
    int delayedTimeMS = 0;
    try {
      // Return immediately if the protocol is not HTTP or the request did not
      // fail.
      if (!context.isWireProtocol(HttpWireProtocol.class) || !context.hasRequestFailed()) {
        return;
      }

      boolean foundActionableFailure = false;
      // We have a Failed HTTP request.
      // Get the exception (Throwable) to find the Http response code.
      Throwable failure = context.getRequestFailure();
      HttpError error = failure.getClass().getAnnotation(HttpError.class);
      if (error != null) {
        int code = error.httpCode();
        if (code >= 500) {
          foundActionableFailure = true;
        }
      } else {
        foundActionableFailure = true;
      }

      // If we have an internal failure...
      if (foundActionableFailure) {
        // Update Stats needs to synchronize on a lock for thread safety.
        // This does not cause performance problems because it only happens
        // on failures, which we may need to delay anyway.
        updateStats();

        // Is the failure rate high enough yet?
        if (failureRate <= allowedPerSecond) return;

        // Determine delay, converting from seconds to milliseconds.
        long elapsed = (long) (context.getCurrentRequestLifetimeSeconds() * 1000);
        if (elapsed < delay) {
          delayedTimeMS = (int) (delay - elapsed);
          log.debug("Sleeping: " + delayedTimeMS);
          Thread.sleep(delayedTimeMS);
        }
      }
    } finally {
      Metrics metrics = context.getMetrics();
      metrics.addTime("NeutronDelay", delayedTimeMS, SI.MILLI(SI.SECOND));
    }
  }

  // This updates the current failureRate using a simple exponential decay algorithm.
  // This operates using milliseconds and assumes one request at a time, thus the lock.
  // Lock contention is not really a problem, since it was add a delay that we probably
  // will want anyway.
  private void updateStats() {
    synchronized (lock) {
      long now = currentTimeMillis();
      failureRate = 1 + Math.exp((last - now) / 1000.0) * failureRate;
      last = now;
    }
  }

  /**
   * Used for unit testing.
   */
  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  /**
   * @see NeutronHandler
   */

  public static class Config {
    private long delay;
    private int allowedPerSecond;

    public long getDelay() {
      return delay;
    }

    public void setDelay(long delay) {
      this.delay = delay;
    }

    public int getAllowedPerSecond() {
      return allowedPerSecond;
    }

    public void setAllowedPerSecond(int allowedPerSecond) {
      this.allowedPerSecond = allowedPerSecond;
    }
  }
}
