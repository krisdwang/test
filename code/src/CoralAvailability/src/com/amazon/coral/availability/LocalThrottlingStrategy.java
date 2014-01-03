// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import com.amazon.coral.util.CharSequenceHashMap;
import com.amazon.coral.util.LexographicalComparator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@code LocalThrottlingStrategy} employs a simple rate-based algorithm.
 * This allows any service instance to have some basic protection against
 * an unexpected burst of requests.
 *
 * @author Eric Crahen &lt;crahen@amazon.com&gt;
 * @version 1.0
 * @Deprecated prefer LocalThrottler
 */
@Deprecated
public class LocalThrottlingStrategy implements ThrottlingStrategy {

  private final static Rate DEFAULT_RATE = new Rate(Double.MAX_VALUE);
  private final static String DEFAULT_KEY = "";
  private final long RESOLUTION = 1; // Accuracy in seconds

  private final Log log = LogFactory.getLog(LocalThrottlingStrategy.class);
  private final Map<CharSequence, Rate> statistics = new CharSequenceHashMap<Rate>();

  /**
   */
  public LocalThrottlingStrategy(Map<String, String> configuration) {

    if(configuration == null)
      throw new IllegalArgumentException();

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
  protected LocalThrottlingStrategy() {
  }

  /**
   */
  @Override
  public boolean isThrottled(CharSequence key) {

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

    if(log.isDebugEnabled()) {
	log.debug("n=" + n + "/" + RESOLUTION + " for key '" + key + "' which has a limit of " + rate.limit);
    }

    return n > rate.limit;

  }

  /**
   */
  protected Rate getRate(CharSequence key) {
    return statistics.get(key);
  }

  /**
   */
  protected void putRate(CharSequence key, Rate rate) {
  }

  /**
   */
  protected long getNanoTimeNow() {
    return System.nanoTime();
  }

  // A tuple that keeps track of the Rate data
  static class Rate implements Serializable {
    private static final long serialVersionUID = 1L;
    private final double limit;   // Maximum rate (request/second)
    private long reset = 0;       // Reset at (nanosecond)
    private long count;           // Count for this interval (request)
    public Rate(double limit) {
      this.limit = limit;
    }
    @Override
    public String toString() {
      return "" + limit + "/s";
    }
  }

}
// Last modified by RcsDollarAuthorDollar on RcsDollarDateDollar
