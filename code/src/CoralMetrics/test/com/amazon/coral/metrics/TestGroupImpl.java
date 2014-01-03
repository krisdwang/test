package com.amazon.coral.metrics;

import java.util.*;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestGroupImpl {
  
  @Test
  public void attribute() {
    GroupBuilder gb = new GroupBuilder();
    gb.setAttribute("abc","def");
    Group g = gb.newGroup();
    
    assertEquals("", g.getName());
    Iterator<String> iter = g.getAttributeNames();
    assertTrue(iter.hasNext());
    assertEquals("abc", iter.next());
    assertEquals("def", g.getAttributeValue("abc"));
  }

  @Test
  public void getMissingAttribute() {
    Group g = new GroupBuilder().newGroup();
    assertNull(g.getAttributeValue("abc"));
  }
  
  @Test
  public void emptyAttributeNameIterator() {
    Group g = new GroupBuilder().newGroup();
    assertFalse(g.getAttributeNames().hasNext());
  }
  
  @Test
  public void compareSameObject() {
    Group g = new GroupBuilder().newGroup();
    
    assertEquals(0, g.compareTo(g));
  }
  
  @Test
  public void compareOnNameLess() {
    Group g1 = new GroupBuilder("abc").newGroup();
    Group g2 = new GroupBuilder("def").newGroup();
                                                                               
    assertTrue(g1.compareTo(g2) < 0);
  }
                                                                               
  @Test
  public void compareOnNameGreater() {
    Group g1 = new GroupBuilder("def").newGroup();
    Group g2 = new GroupBuilder("abc").newGroup();
    
    assertTrue(g1.compareTo(g2) > 0);
  }

  private static class IncompatibleGroup implements Group {
    public String getName() { return null; }
    public String getAttributeValue(String name) { return null; }
    public Iterator<String> getAttributeNames() { return null; }
    public int compareTo(Group g) { return 1; }
    public boolean equals(Object obj) { return false; }
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void compareIncompatibleGroup() {
    Group normalGroup = new GroupBuilder().newGroup();
    Group incompatibleGroup = new IncompatibleGroup();
    normalGroup.compareTo(incompatibleGroup);
  }
                                                                                 
  @Test
  public void compareOnNameEqual() {
    Group g1 = new GroupBuilder("abc").newGroup();
    Group g2 = new GroupBuilder("abc").newGroup();
                                                                               
    assertEquals(0, g1.compareTo(g2));
  }
                                                                               
  @Test
  public void compareSort() {
    Group g1 = new GroupBuilder("abc").newGroup();
    Group g2 = new GroupBuilder("def").newGroup();
    
    Group[] array = new Group[2];
    array[0] = g2;
    array[1] = g1;
    
    // check the natural ordering
    Arrays.sort(array);
    
    assertEquals(g1, array[0]);
    assertEquals(g2, array[1]);
  }
  
  @Test
  public void compareUnequalAttributeCounts() {
    Group g1 = new GroupBuilder("abc").newGroup();
    Group g2 = new GroupBuilder("abc").setAttribute("def","ghi").newGroup();
    
    // g1 is less than g2
    assertTrue(g1.compareTo(g2) < 0);
    assertTrue(g2.compareTo(g1) > 0);
  }

  @Test
  public void compareEqualAttributeCounts() {
    Group g1 = new GroupBuilder("abc").setAttribute("def","ghi").newGroup();
    Group g2 = new GroupBuilder("abc").setAttribute("def","klm").newGroup();

    // g1 is less than g2
    assertTrue(g1.compareTo(g2) < 0);
    assertTrue(g2.compareTo(g1) > 0);
  }

  @Test
  public void compareOnAttributesEqual() {
    Group g1 = new GroupBuilder("abc").setAttribute("def","ghi").setAttribute("klm","nop").newGroup();
    Group g2 = new GroupBuilder("abc").setAttribute("def","ghi").setAttribute("klm","nop").newGroup();
    
    assertTrue(g1.compareTo(g2) == 0);
    assertTrue(g2.compareTo(g1) == 0);
  }
  
  @Test
  public void compareMismatchedAttributeKeys() {
    Group g1 = new GroupBuilder("abc").setAttribute("def","ghi").newGroup();
    Group g2 = new GroupBuilder("abc").setAttribute("klm","ghi").newGroup();
    
    assertTrue(g1.compareTo(g2) < 0);
    assertTrue(g2.compareTo(g1) > 0);
  }

  @Test
  public void hashCodeNameOnly() {
    String name = "abc";
    Group g = new GroupBuilder(name).newGroup();
                                                                               
    assertEquals(name.hashCode(), g.hashCode());
  }

  @Test
  public void hashCodeAttributes() {
    String name = "abc";
    String attrName = "def";
    String attrValue = "ghi";
    Group g = new GroupBuilder(name).setAttribute(attrName,attrValue).newGroup();
    
    assertEquals(name.hashCode() + attrName.hashCode() * attrValue.hashCode(), g.hashCode());
  }

  @Test
  public void equalsSameObject() {
    Group g = new GroupBuilder().newGroup();
    
    assertTrue(g.equals(g));
  }
                                                                               
  @Test
  public void equalsNamesMatch() {
    Group g1 = new GroupBuilder("abc").newGroup();
    Group g2 = new GroupBuilder("abc").newGroup();
                                                                               
    assertTrue(g1.equals(g2));
  }
                                                                               
  @Test
  public void equalsNamesDontMatch() {
    Group g1 = new GroupBuilder("abc").newGroup();
    Group g2 = new GroupBuilder("").newGroup();
                                                                               
    assertFalse(g1.equals(g2));
  }

  @Test
  public void equalsTypeMismatch() {
    Group g = new GroupBuilder().newGroup();
    Object obj = new Object();
    
    assertFalse(g.equals(obj));
  }

  @Test(expected=UnsupportedOperationException.class)
  public void preventIteratorRemove() {
    Group g = new GroupBuilder().setAttribute("abc","def").newGroup();
    Iterator<String> iter = g.getAttributeNames();
    iter.next();
    iter.remove();
  }
}
