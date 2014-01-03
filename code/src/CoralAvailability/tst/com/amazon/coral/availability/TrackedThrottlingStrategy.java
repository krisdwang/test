package com.amazon.coral.availability;

import java.util.List;
import java.util.LinkedList;

public class TrackedThrottlingStrategy implements ThrottlingStrategy {
  private final ThrottlingStrategy delegate;
  private final List<String> keys = new LinkedList<String>();

  public TrackedThrottlingStrategy() {
    this(new ThrottlingStrategy() {
      @Override public boolean isThrottled(CharSequence key) { return false; }
    });
  }

  public TrackedThrottlingStrategy(ThrottlingStrategy strategy) {
    if (strategy == null)
      throw new IllegalArgumentException();
    this.delegate = strategy;
  }

  public List<String> getSeenKeys() {
    return keys;
  }

  @Override
  public boolean isThrottled(CharSequence key) {
    keys.add(key.toString());
    return delegate.isThrottled(key);
  }
}
