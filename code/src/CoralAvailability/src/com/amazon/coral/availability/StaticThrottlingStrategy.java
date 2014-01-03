// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;

import com.amazon.coral.util.CharSequenceHashSet;

/** @Deprecated prefer StaticThrottler */
@Deprecated
public class StaticThrottlingStrategy implements ThrottlingStrategy {
  private final boolean isBlacklist;
  private final Set<CharSequence> keys;

  private StaticThrottlingStrategy(Collection<CharSequence> keys, boolean isBlacklist) {
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
  public boolean isThrottled(CharSequence key) {
    if (isBlacklist) {
      return keys.contains(key);
    }
    return !keys.contains(key);
  }

  public static StaticThrottlingStrategy newBlacklist(Collection<CharSequence> keys) {
    return new StaticThrottlingStrategy(keys, true);
  }

  public static StaticThrottlingStrategy newWhitelist(Collection<CharSequence> keys) {
    return new StaticThrottlingStrategy(keys, false);
  }
}
