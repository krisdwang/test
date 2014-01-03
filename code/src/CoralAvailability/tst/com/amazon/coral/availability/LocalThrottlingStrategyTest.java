// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import com.amazon.coral.service.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.*;

public class LocalThrottlingStrategyTest {

  @Test
  public void checkDefault() {
    // We assume this equality elsewhere.
    assertEquals("", ThrottlingHandler.DEFAULT_KEY);
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullMap() {
    new LocalThrottlingStrategy(null);
  }

  @Test
  public void defaultIsUnlimited() {
    ThrottlingStrategy s = new LocalThrottlingStrategy(new HashMap<String,String>());
    assertFalse( s.isThrottled("whatever") );
  }

  @Test
  public void throttleA() {

    ThrottlingStrategy s = new LocalThrottlingStrategy(new HashMap<String,String>(){{
      put("a", "0"); // never allows a
    }});

    assertTrue( s.isThrottled("a") );
    // unknown key - should not get throttled.
    assertFalse( s.isThrottled("b") );

  }

  @Test
  public void throttleEmptyConstructor() {
    ThrottlingStrategy s = new LocalThrottlingStrategy(new HashMap<String, String>());
    assertFalse( s.isThrottled("") );
  }

  @Test
  public void throttleCatchAll() {
    ThrottlingStrategy s = new LocalThrottlingStrategy(new HashMap<String, String>(){{
      put("", "0");
    }});
    assertTrue( s.isThrottled("") );
  }

  @Test
  public void throttleAoverTime() {

    final long time[] = new long[] { 1 };
    // 2 requests, per second
    ThrottlingStrategy s = new LocalThrottlingStrategy(new HashMap<String,String>(){{ put("a", "2"); }}) {
      @Override
      protected long getNanoTimeNow() {
        return time[0];
      }
    };

    assertFalse( s.isThrottled("a") );
    assertFalse( s.isThrottled("a") );
    assertTrue( s.isThrottled("a") );

    time[0]+=3*1000000000L; // Minimum resolution

    assertFalse( s.isThrottled("a") );
    assertFalse( s.isThrottled("a") );
    assertTrue( s.isThrottled("a") );

  }

}
