package com.amazon.coral.metrics;

import java.util.*;

public class GroupBuilder {

  private final String groupName;
  private final ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
  // used to set appropriate array type for creating the attribute array
  private static final Attribute[] dummyArray = new Attribute[0];

  /**
   * A default, unqualified group object, equivalent to
   * <code>new GroupBuilder().newGroup()</code>
   * This group has no attributes and is named "" (empty string)
   */
  public static final Group DEFAULT_GROUP = new GroupBuilder().newGroup();

  /**
   * Groups used for metrics intended to be sent to Metering.  The 
   * MeteringReporter acts on metrics of this group.
   */
  public static final Group METERING_GROUP = new GroupBuilder("Metering").newGroup();
  public static final Group BANDWIDTH_METERING_GROUP = new GroupBuilder("BandwidthMetering").newGroup();


  /**
   * Creates a GroupBuilder for the default group name "" (empty string)
   */
  public GroupBuilder() {
    this.groupName = "";
  }

  /**
   * Creates a GroupBuilder for the specified group name.  Group names must be 
   * non-null.
   *
   * @param name the name of the group to build
   */
  public GroupBuilder(String groupName) {
    if(groupName == null)
      throw new IllegalArgumentException("Group names may not be null");
    this.groupName = groupName;
  }

  /**
   * Sets a value for the specified attribute name.
   *
   * @param name the attribute name (key) to set
   * @param value the value to associate with the specified name
   */
  public GroupBuilder setAttribute(String name, String value) {
    if(name == null || name.equals(""))
      throw new IllegalArgumentException("Attribute names may not be null");
    if(value == null)
      throw new IllegalArgumentException("Attribute values may not be null");
    
    attributeList.add(new Attribute(name, value));

    return this;
  }

  /**
   * Instantiates a group with the name and attributes specified in this GroupBuilder
   * 
   * @return a new group object as described by this builder
   */
  public Group newGroup() {
    return new GroupImpl(groupName, attributeList.toArray(dummyArray));
  }




  // Defines a simple key/value pair for Attribute storage
  private static class Attribute {
    String name;
    String value;
    
    Attribute(String name, String value) {
      this.name = name;
      this.value = value;
    }
    
    public int hashCode() {
      return this.name.hashCode() * this.value.hashCode();
    }
  }

  // The Group-implementing object class return by GroupBuilder.newGroup().
  // Stores attributes in a sorted array for rapid comparisons and lookups.
  // Attributes are sorted by name; lookups are performed by binary search
  private static class GroupImpl implements Group {

    // performs comparison of attributes using only their names
    // used for sorting and searching for attribute values
    private static class AttributeSortComparator implements Comparator<Attribute> {
      public int compare(Attribute a1, Attribute a2) {
        return a1.name.compareTo(a2.name);
      }
    }
    
    // performs comparison of attributes using both names and values, names having greater significance
    // this comparator is used for comparing GroupImpl instances with one another
    private static class AttributeEqualityComparator implements Comparator<Attribute> {
      public int compare(Attribute a1, Attribute a2) {
        int rv = a1.name.compareTo(a2.name);
        if(rv != 0) 
          return rv;
        else
          return a1.value.compareTo(a2.value);
      }
    }

    // the iterator returned by GroupImpl.getAttributeNameIterator()
    // walks the attribute array in order, disables the use of the iterator's optional .remove() method
    private class AttributeNameIterator implements Iterator<String> {
      int index = 0;
      
      public boolean hasNext() {
        return (index < sortedAttributes.length);
      }
      public String next() {
        String output = sortedAttributes[index].name;
        index++;
        return output;
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    // instances of the above-defined comparators
    private static final Comparator<Attribute> attribSortComparator = new AttributeSortComparator();
    private static final Comparator<Attribute> attribEqualityComparator = new AttributeEqualityComparator();

    private String name;
    private Attribute[] sortedAttributes;
    
    private GroupImpl(String name, Attribute[] attributes) {
      this.name = name;
      this.sortedAttributes = attributes;

      Arrays.sort(this.sortedAttributes, attribSortComparator);
    }

    public String getName() {
      return name;
    }

    public String getAttributeValue(String name) {
      // the sort comparator ignores values while searching, so we supply "" to 
      // construct the object
      Attribute a = new Attribute(name, "");
      int index = Arrays.binarySearch(sortedAttributes, a, attribSortComparator);
      if(index < 0)
        return null;
      else
        return sortedAttributes[index].value;
    }

    public Iterator<String> getAttributeNames() {
      return new AttributeNameIterator();
    }

    public int compareTo(Group group) {
      if(this == group)
        return 0;

      if(!(group instanceof GroupImpl))
        throw new IllegalArgumentException("GroupImpl can only compare with other GroupImpl objects");
      
      GroupImpl groupImpl = (GroupImpl)group;
      
      int rv = this.name.compareTo(groupImpl.name);
      if(rv != 0)
        return rv;

      // check that we have the same length attribute sets
      // if not, the shorter sets are 'less' than the longer ones
      rv = this.sortedAttributes.length - groupImpl.sortedAttributes.length;
      if(rv != 0)
        return rv;

      for(int i = 0; i < sortedAttributes.length; i++ ) {
        rv = attribEqualityComparator.compare(this.sortedAttributes[i], groupImpl.sortedAttributes[i]);
        if(rv != 0) {
          return rv;
        }
      }
      
      // all tests passed, the groups are equal
      return 0;
    }

    public int hashCode() {
      int output = name.hashCode();
      
      for(int i = 0; i < sortedAttributes.length; i++) {
        output += sortedAttributes[i].hashCode();
      }
      
      return output;
    }

    public boolean equals(Object obj) {
      if(!(obj instanceof GroupImpl))
        return false;
      else
        return (this.compareTo((GroupImpl)obj) == 0);
    }
  }
}
