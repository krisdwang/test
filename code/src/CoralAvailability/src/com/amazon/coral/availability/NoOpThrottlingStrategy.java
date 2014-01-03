// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

/** @Deprecated prefer NoOpThrottler */
@Deprecated
public class NoOpThrottlingStrategy implements ThrottlingStrategy {

  public NoOpThrottlingStrategy() {
  }

  @Override
  public boolean isThrottled(CharSequence key) {
      return false;
  }

}
