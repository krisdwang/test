// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import org.junit.Test;
import org.junit.Assert;

public class NoOpThrottlingStrategyTest {
  public static ThrottlingStrategy create() {
    return new NoOpThrottlingStrategy();
  }
  @Test
  public void alwaysOff() {
    ThrottlingStrategy s = create();
    Assert.assertFalse(s.isThrottled(""));
    Assert.assertFalse(s.isThrottled(""));
  }
}
