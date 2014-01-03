package com.amazon.coral.metrics;

import java.util.Iterator;

/**
 * A <tt>Group</tt> instance is an identifier for a logical group of <tt>Metrics</tt>
 * primitives.  It is used in the instantiation of <tt>Metrics</tt> objects via the
 * <code>Metrics.newMetrics(Group group)</code> call.  Group instances consist of a name
 * and a set of attributes that qualify the metrics associated with the group.
 *
 * <tt>Group</tt> instances are immutable objects created by a <tt>GroupBuilder</tt>
 * object.  
 *
 * @see GroupBuilder
 *
 * @author Matt Wren <wren@amazon.com>
 * @version 1.0
 */
public interface Group extends Comparable<Group> {
  /**
   * Retrieves the name of this group.
   *
   * @return the group name
   */
  public String getName();

  /**
   * Retrieves the value of a specific attribute.  If the specified attribute name is
   * present in this group, its value is returned.  If it is not present, null is 
   * returned.
   *
   * @param name attribute name
   * @return the associated value or null if not present
   */
  public String getAttributeValue(String name);

  /**
   * Generates an iterator that can be used to access the names of all attributes
   * associated with this group.  The returned iterator does not permit modification
   * of the attributes themselves.
   */
  public Iterator<String> getAttributeNames();
  
  /**
   * Tests this group for equality with another group, returning true only if both groups
   * have an identical name and set of attribute name/value pairs.
   *
   * @param obj the object to test
   * @return true if the Groups match, false otherwise
   */
  public boolean equals(Object obj);

  /**
   * Generates a hash code for this group.
   *
   * @return a hash code
   */
  public int hashCode();
}
