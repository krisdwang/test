// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.service.AbstractHandler;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.util.CharSequenceHashMap;

/**
 * The intention of this class is to cache potentially expensive throttling checks against
 * either this.strategy OR this.throttler.
 * @author yamanoha
 * @deprecated Prefer com.amazon.coral.avail.throttlers.MemoizedThrottler
 */
@Deprecated
public class MemoizedThrottlingStrategy extends Throttler implements ThrottlingStrategy {
  private final static Log log = LogFactory.getLog(MemoizedThrottlingStrategy.class);

  /** If a memoizer is used twice by 2 different Handlers, we will issue
   * a warning. But just once for the duration of the program. */
  private static boolean youveBeenWarned = false;


  /** This handler created (or at least first used) this MemoizedThrottlingStrategy. */
  private AbstractHandler firstUsedBy = null;

  /**
   * A cache of throttling decisions based on key.
   */
  private final CharSequenceHashMap<Boolean> cache = new CharSequenceHashMap<Boolean>();
  /**
   * The strategy whose decisions are cached within this.cache.
   */
  @Deprecated
  private final ThrottlingStrategy strategy;

  /**
   * The throttler whose decisions are cached within this.cache.
   */
  private final Throttler throttler;
  /**
   * A null metrics implementation for invocations of isThrottled(CharSequence key).
   */
  private final Metrics nullMetrics;

  /**
   * Constructs a cached throttler implemented as a strategy.
   * @param strategy The strategy to cache throttling decisions against.
   */
  @Deprecated
  public MemoizedThrottlingStrategy(ThrottlingStrategy strategy) {
    if(strategy == null)
      throw new IllegalArgumentException();
    this.strategy = strategy;
    this.throttler = null;
    this.nullMetrics = new NullMetricsFactory().newMetrics();
  }

  MemoizedThrottlingStrategy(ThrottlingStrategy strategy, AbstractHandler handler) {
    this(strategy);
  }

  /**
   * Constructs a cached throttler implemented as a throttler.
   * @param throttler The throttler to cache throttling decisions against.
   */
  public MemoizedThrottlingStrategy(Throttler throttler) {
    if(throttler == null)
      throw new IllegalArgumentException();
    this.strategy = null;
    this.throttler = throttler;
    this.nullMetrics = new NullMetricsFactory().newMetrics();
  }

  /** Handlers call this whenever they use this Memoizer. If two different
   * handlers use the same Memoizer, warn, but only once for the duration
   * of the entire program.
   */
  void usedBy(final AbstractHandler handler) {
    if (youveBeenWarned) return;
    if (null == this.firstUsedBy) {
      this.firstUsedBy = handler;
    } else if (handler != this.firstUsedBy) {
      log.warn("Two handlers (probably throttling or load-shed) use the same MEMOIZER. "
        + "You might have a bug. See https://w.amazon.com/index.php/Coral/Throttle/Bugs/Memoized");
      youveBeenWarned = true;
    }
  }

  /**
   * This method will examine the information provided and make a decision
   * about whether or not throttling should occur.
   */
  @Deprecated
  public boolean isThrottled(CharSequence key) {
    return isThrottled(key, this.nullMetrics);
  }

  @Override
  public boolean isThrottled(CharSequence key, Metrics metrics) {
    // Throttling decision first depends on the cached state
    if(!cache.containsKey(key)) {
      // If a decision was not previously cached, do so now based on which system
      // the memozier was initialized with (strategy or throttler).
      if(this.strategy != null) { // <---- HANDLE THROTTLING_STRATEGY MEMOIZATION
        cache.put(key, this.strategy.isThrottled(key));
      } else { // <---- HANDLE THROTTLER MEMOIZATION
        cache.put(key, this.throttler.isThrottled(key, metrics));
      }
    }
    return cache.get(key);
  }
}
