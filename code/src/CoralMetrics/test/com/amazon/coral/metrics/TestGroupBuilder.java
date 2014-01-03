package com.amazon.coral.metrics;

import java.util.*;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestGroupBuilder {

  @Test
  public void defaultGroup() {
    GroupBuilder gb = new GroupBuilder();
    Group g = gb.newGroup();

    assertEquals("", g.getName());
    // check no attributes exist
    assertFalse(g.getAttributeNames().hasNext());
  }

  @Test
  public void name() {
    GroupBuilder gb = new GroupBuilder("abc");
    Group g = gb.newGroup();

    assertEquals("abc", g.getName());
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullName() {
    GroupBuilder gb = new GroupBuilder(null);
  }


  @Test(expected=IllegalArgumentException.class)
  public void nullAttributeName() {
    GroupBuilder gb = new GroupBuilder();
    gb.setAttribute(null,"abc");
  }

  @Test(expected=IllegalArgumentException.class)
  public void emptyAttributeName() {
    GroupBuilder gb = new GroupBuilder();
    gb.setAttribute("","abc");
  }

  @Test(expected=IllegalArgumentException.class)
  public void nullAttributeValue() {
    GroupBuilder gb = new GroupBuilder();
    gb.setAttribute("abc",null);
  }

}
