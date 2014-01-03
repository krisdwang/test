// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.metrics.Metrics;

/**
 * A {@code CompositeThrottler} allows for some redundancy.
 */
public class CompositeThrottler extends Throttler {

  private final Throttler[] throttler;

  public CompositeThrottler(Throttler... throttler) {
    if(throttler == null)
      throw new IllegalArgumentException();
    for(Throttler s : throttler)
      if(s == null)
        throw new IllegalArgumentException();
    this.throttler = throttler.clone();
  }

  /**
   */
  public boolean isThrottled(CharSequence key, Metrics m) {
    for(Throttler s : throttler)
      if(s.isThrottled(key, m))
        return true;
    return false;
  }

}
