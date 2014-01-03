// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import java.util.List;
import java.util.LinkedList;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.throttle.api.Throttler;

public class TrackedThrottler extends Throttler {
  private final Throttler referent;
  private final List<String> keys = new LinkedList<String>();

  public TrackedThrottler() {
    this(new Throttler() {
      @Override public boolean isThrottled(CharSequence key, Metrics m) { return false; }
    });
  }

  public TrackedThrottler(Throttler strategy) {
    if (strategy == null)
      throw new IllegalArgumentException();
    this.referent = strategy;
  }

  public List<String> getSeenKeys() {
    return keys;
  }

  @Override
  public boolean isThrottled(CharSequence key, Metrics m) {
    keys.add(key.toString());
    return referent.isThrottled(key, m);
  }
}
