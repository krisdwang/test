package com.amazon.coral.availability;

/**
 * A {@code CompositeThrottlingStrategy} allows for some redundancy.
 *
 * @author Eric Crahen &lt;crahen@amazon.com&gt;
 * @version 1.0
 * @Deprecated Prefer CompositeThrottler
 */
@Deprecated
public class CompositeThrottlingStrategy implements ThrottlingStrategy {

  private final ThrottlingStrategy[] strategy;

  public CompositeThrottlingStrategy(ThrottlingStrategy... strategy) {
    if(strategy == null)
      throw new IllegalArgumentException();
    for(ThrottlingStrategy s : strategy)
      if(s == null)
        throw new IllegalArgumentException();
    this.strategy = strategy.clone();
  }

  /**
   */
  public boolean isThrottled(CharSequence key) {
    for(ThrottlingStrategy s : strategy)
      if(s.isThrottled(key))
        return true;
    return false;
  }

}
// Last modified by RcsDollarAuthorDollar on RcsDollarDateDollar
