package com.amazon.coral.availability;

import org.junit.Test;
import static org.junit.Assert.*;

public class CompositeThrottlingStrategyTest {

  @Test(expected=IllegalArgumentException.class)
  public void nullStrategies() {
    ThrottlingStrategy[] s = null;
    new CompositeThrottlingStrategy(s);
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullStrategy() {
    new CompositeThrottlingStrategy(new ThrottlingStrategy[] { null });
  }

  @Test
  public void delegates() {
    final int count[] = new int[]{0};
    ThrottlingStrategy s = new ThrottlingStrategy() {
      @Override
      public boolean isThrottled(CharSequence key) {
        count[0]++;
        return true;
      }
    };
    s = new CompositeThrottlingStrategy(s);
    s.isThrottled("a");
    assertEquals(1, count[0]);
  }

}
