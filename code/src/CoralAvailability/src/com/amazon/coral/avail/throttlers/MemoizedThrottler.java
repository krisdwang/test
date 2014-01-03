// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import com.amazon.coral.google.common.base.Preconditions;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.util.CharSequenceHashMap;

/**
 * The intention of this class is to cache potentially expensive throttling checks against
 * this.throttler.
 * @author yamanoha
 *
 */
public class MemoizedThrottler extends Throttler {
  /**
   * A cache of throttling decisions based on key.
   */
  private final CharSequenceHashMap<Boolean> cache = new CharSequenceHashMap<Boolean>();

  /**
   * The throttler whose decisions are cached within this.cache.
   */
  private final Throttler throttler;

  /**
   * Constructs a cached throttler implemented as a throttler.
   * @param throttler The throttler to cache throttling decisions against.
   */
  public MemoizedThrottler(Throttler throttler) {
    Preconditions.checkNotNull(throttler);
    this.throttler = throttler;
  }

  @Override
  public boolean isThrottled(CharSequence key, Metrics metrics) {
    if(!cache.containsKey(key)) {
      cache.put(key, this.throttler.isThrottled(key, metrics));
    }
    return cache.get(key);
  }
}
