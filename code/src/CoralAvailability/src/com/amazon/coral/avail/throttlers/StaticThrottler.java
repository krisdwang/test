// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;

import com.amazon.coral.util.CharSequenceHashSet;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.throttle.api.Throttler;

public class StaticThrottler extends Throttler {
  private final boolean isBlacklist;
  private final Set<CharSequence> keys;

  private StaticThrottler(Collection<CharSequence> keys, boolean isBlacklist) {
    if (keys == null)
      throw new IllegalArgumentException();
    this.isBlacklist = isBlacklist;
    this.keys = new CharSequenceHashSet<CharSequence>();
    for (CharSequence key : keys) {
      if (key == null)
        throw new IllegalArgumentException();
      this.keys.add(key.toString());
    }
  }

  @Override
  public boolean isThrottled(CharSequence key, Metrics m) {
    if (isBlacklist) {
      return keys.contains(key);
    }
    return !keys.contains(key);
  }

  public static StaticThrottler newBlacklist(Collection<CharSequence> keys) {
    return new StaticThrottler(keys, true);
  }

  public static StaticThrottler newWhitelist(Collection<CharSequence> keys) {
    return new StaticThrottler(keys, false);
  }
}
