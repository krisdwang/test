package com.amazon.coral.availability;

import java.util.List;
import java.util.LinkedList;

import org.junit.Test;
import static org.junit.Assert.*;

public class StaticThrottlingStrategyTest {

  @Test(expected=IllegalArgumentException.class)
  public void nullBlacklist() {
    StaticThrottlingStrategy.newBlacklist(null);
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullWhitelist() {
    StaticThrottlingStrategy.newWhitelist(null);
  }

  @Test
  public void blacklist() {
    List<CharSequence> keys = new LinkedList<CharSequence>();
    keys.add("foo");
    keys.add("bar");
    ThrottlingStrategy strategy = StaticThrottlingStrategy.newBlacklist(keys);
    assertTrue(strategy.isThrottled("foo"));
    assertTrue(strategy.isThrottled("bar"));

    StringBuilder sb = new StringBuilder();
    sb.append("fo");
    sb.append("o");
    assertTrue(strategy.isThrottled(sb));

    assertFalse(strategy.isThrottled("foobar"));
    assertFalse(strategy.isThrottled(""));
  }

  @Test
  public void whitelist() {
    List<CharSequence> keys = new LinkedList<CharSequence>();
    keys.add("foo");
    keys.add("bar");
    ThrottlingStrategy strategy = StaticThrottlingStrategy.newWhitelist(keys);
    assertFalse(strategy.isThrottled("foo"));
    assertFalse(strategy.isThrottled("bar"));
    assertTrue(strategy.isThrottled("foobar"));
    assertTrue(strategy.isThrottled(""));
  }

}
