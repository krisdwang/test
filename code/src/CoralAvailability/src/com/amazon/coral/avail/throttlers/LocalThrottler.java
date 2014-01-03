// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.google.common.base.Preconditions;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.util.CharSequenceHashMap;
import com.amazon.coral.util.LexographicalComparator;

/**
 * A {@code LocalThrottler} employs a simple rate-based algorithm.
 * This allows any service instance to have some basic protection against
 * an unexpected burst of requests.
 */
public class LocalThrottler extends Throttler {
  private final static Rate DEFAULT_RATE = new Rate(Double.MAX_VALUE);
  private final static String DEFAULT_KEY = com.amazon.coral.avail.flow.ThrottlingHandler.DEFAULT_KEY;
  private final long RESOLUTION = 1; // Accuracy in seconds

  private final Log log = LogFactory.getLog(LocalThrottler.class);
  private final Map<CharSequence, Rate> statistics = new CharSequenceHashMap<Rate>();

  public static class Builder {
    private Map<String, String> keymap;

    public void setMap(Map<String, String> keymap) {
      this.keymap = keymap;
    }
    public LocalThrottler build() {
      Preconditions.checkNotNull(keymap, "Map is required.");
      return new LocalThrottler(keymap);
    }
  }

  /**
   * Temporarily public, for an old test.
   */
  LocalThrottler(Map<String, String> configuration) {
    statistics.put(DEFAULT_KEY, DEFAULT_RATE);
    for(Map.Entry<String, String> e : configuration.entrySet()) {
      String key = e.getKey();
      try {
        double limit = Double.valueOf(e.getValue());
        if(limit < 0)
          throw new IllegalArgumentException(key + " has invalid rate " + limit);
        statistics.put(key, new Rate(limit));
      } catch(NumberFormatException x) {
        throw new IllegalArgumentException(x);
      }
    }

    // Log something useful for people trying to understand their service
    if(log.isInfoEnabled()) {
      TreeMap<CharSequence, Rate> m = new TreeMap<CharSequence, Rate>(LexographicalComparator.CASE_INSENSITIVE_ORDER);
      m.putAll(statistics);
      log.info("Throttling Rate Table: " + m);
    }

  }

  /** Strictly for derived classes which plan to ignore the 'statistics' map.
   */
  protected LocalThrottler() {
  }

  /**
   * 'null' means "do not throttle"
   */
  @Override
  public boolean isThrottled(CharSequence key, Metrics metrics) {

    final Rate rate = getRate(key);

    // if key is not available, there is nothing much
    // we can do about it. just don't throttle.
    if(null == rate)
      return false;

    if(rate.limit == 0)
      return true;

    double n;

    synchronized(rate) {

      // Get the rate data for the current time-slice
      long now = getNanoTimeNow();
      if(rate.reset < now) {
        rate.reset = now + RESOLUTION*TimeUnit.SECONDS.toNanos(1);
        rate.count = 0;
      }

      // Convert the count/resolution into count/second
      n = (double)++rate.count;
      n /= RESOLUTION;

    }

    putRate(key, rate);

    if (key.equals(DEFAULT_KEY)) {
      metrics.addLevel("ThrottlingProximityDefault", n / rate.limit, Unit.ONE, 1);
    } else {
      metrics.addLevel("ThrottlingProximity", n / rate.limit, Unit.ONE, 1);
    }
    if(log.isDebugEnabled()) {
      log.debug("n=" + n + "/" + RESOLUTION + " for key '" + key + "' which has a limit of " + rate.limit);
    }
    return n > rate.limit;

  }

  /**
   * Returns the Rate for the given key.
   */
  public Rate getRate(CharSequence key) {
    return statistics.get(key);
  }

  /**
   */
  protected void putRate(CharSequence key, Rate rate) {
  }

  /**
   * Used to update the throttling limit (in tps) of the given key.
   * Any current count associated with the key is retained.
   * There could be minor count loss due to synchronization issues,
   * but slight loss of accuracy is acceptable to retain performance.
   */
  public void setLimit(CharSequence key, double limit) {
    if (limit < 0) {
      throw new IllegalArgumentException(key + " has invalid rate " + limit);
    }
    Rate newRate = new Rate(limit);
    Rate oldRate = statistics.get(key);
    if (oldRate != null) {
        newRate.count = oldRate.count;
        newRate.reset = oldRate.reset;
    }
    statistics.put(key, newRate);
  }

  /**
   * Used to remove the throttling limit for the given key. 
   */
  public void removeLimit(CharSequence key) {
    statistics.remove(key);
  }

  /**
   * Override this for testing.
   */
  protected long getNanoTimeNow() {
    return System.nanoTime();
  }

  // A tuple that keeps track of the Rate data
  public static class Rate implements Serializable {
    private static final long serialVersionUID = 1L;
    private final double limit;   // Maximum rate (request/second)
    private long reset = 0;       // Reset at (nanosecond)
    private long count;           // Count for this interval (request)
    public Rate(double limit) {
      this.limit = limit;
    }
    public double getLimit() {
      return this.limit;
    }
    @Override
    public String toString() {
      return "" + limit + "/s, " + count;
    }
  }

}
// Last modified by RcsDollarAuthorDollar on RcsDollarDateDollar
