package com.amazon.coral.metrics.reporter;

import java.io.*;
import java.util.*;

class QuerylogDOM {

  final ArrayList<QuerylogRecord> records = new ArrayList<QuerylogRecord>();

  public QuerylogDOM(BufferedReader reader) throws IOException {
    String line;
    while(reader.ready() && (line = reader.readLine()) != null) {
      if(line.indexOf("------------------------------------------------------------------------") == 0)
        addRecord(new QuerylogRecord(reader));
    }
  }

  public QuerylogDOM(String string) throws IOException {
    this(new BufferedReader(new StringReader(string)));
  }

  public QuerylogDOM() {

  }

  public Iterable<QuerylogRecord> getRecords() {
    return records;
  }

  public QuerylogDOM addRecord(QuerylogRecord r) {
    records.add(r);
    return this;
  }

  public boolean equals(Object obj) {
    if(!(obj instanceof QuerylogDOM))
      return false;

    QuerylogDOM qlog = (QuerylogDOM)obj;
    return this.contains(qlog) && qlog.contains(this);
  }

  public void appendTo(Appendable a) throws IOException {
    for( QuerylogRecord record : records ) {
      a.append(record.toString());
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    try { appendTo(sb); } catch(IOException e) { return ""; }
    return sb.toString();
  }

  public boolean contains(QuerylogDOM querylog) {
    Iterator<QuerylogRecord> thatRecords = querylog.records.iterator();
    while(thatRecords.hasNext()) {
      if(this.records.contains(thatRecords.next()) == false)
        return false;
    }

    return true;
  }

  public static class ServiceMetricsMetric {
    protected String name;
    protected String criticality;
    protected String type;
    protected String value;
    protected String repeat;
    protected String units;

    public String getName() {
      return name;
    }
    public String getCriticality() {
      return criticality;
    }
    public String getType() {
      return type;
    }
    public String getValue() {
      return value;
    }
    public String getRepeat() {
      return repeat;
    }
    public String getUnits() {
      return units;
    }

    public void appendTo(Appendable a) throws IOException {
      a.append(name);
      if(criticality.equals("") == false) {
        a.append(':');
        a.append(criticality);
      }
      a.append('#');
      a.append(type);
      a.append('/');
      a.append(value);
      a.append('/');
      a.append(repeat);
      a.append('/');
      a.append(units);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      try { appendTo(sb); } catch(IOException e) { return ""; }
      return sb.toString();
    }

    public boolean equals(Object obj) {
      if(!(obj instanceof ServiceMetricsMetric)) return false;

      ServiceMetricsMetric metric = (ServiceMetricsMetric)obj;

      return this.name.equals(metric.name) &&
        this.criticality.equals(metric.criticality) &&
        this.type.equals(metric.type) &&
        this.value.equals(metric.value) &&
        this.repeat.equals(metric.repeat) &&
        this.units.equals(metric.units);
    }

    public ServiceMetricsMetric(String name, String criticality, String type, String value, String repeat, String units) {
      if(name == null)
        throw new IllegalArgumentException("Name must not be null");
      if(type == null)
        throw new IllegalArgumentException("Type must not be null");
      if(value == null)
        throw new IllegalArgumentException("Value must not be null");
      if(repeat == null)
        throw new IllegalArgumentException("Repeat must not be null");
      if(criticality == null)
        criticality = "";
      if(units == null)
        units = "";

      this.name = name;
      this.criticality = criticality;
      this.type = type;
      this.value = value;
      this.repeat = repeat;
      this.units = units;
    }

    public ServiceMetricsMetric(String entry) {
      int startIdx, endIdx;
      String tempStr;

      startIdx = 0;
      endIdx = entry.indexOf(":", startIdx);
      if( endIdx < 0 || entry.indexOf("#", startIdx) < endIdx ) {
        endIdx = entry.indexOf("#", startIdx );
        this.name = entry.substring(startIdx, endIdx);
        this.criticality = "";
      } else {
        this.name = entry.substring(startIdx, endIdx);

        startIdx = endIdx + 1;
        endIdx = entry.indexOf("#", startIdx);
        this.criticality = entry.substring(startIdx, endIdx);
      }

      String valueStr = entry.substring(endIdx + 1);
      String[] valueComponents = valueStr.split("/");

      type = valueComponents[0];
      value = valueComponents[1];
      repeat = valueComponents[2];
      if(valueComponents.length < 4)
        units = "";
      else
        units = valueComponents[3];
    }
  }

  public static class ServiceMetricsGroup {
    protected final ArrayList<ServiceMetricsMetric> metrics = new ArrayList<ServiceMetricsMetric>();
    protected final ArrayList<String> attributeValues = new ArrayList<String>();

    public void appendTo(Appendable a) throws IOException {
      Iterator<String> iterAttribs = attributeValues.iterator();
      while(iterAttribs.hasNext()) {
        String attribValue = iterAttribs.next();
        a.append(attribValue);
        if(iterAttribs.hasNext())
          a.append(':');
      }
      if(attributeValues.size() > 0)
        a.append('=');

      Iterator<ServiceMetricsMetric> iterMetrics = metrics.iterator();
      while(iterMetrics.hasNext()) {
        ServiceMetricsMetric metric = iterMetrics.next();
        metric.appendTo(a);
        if(iterMetrics.hasNext())
          a.append(',');
      }

      a.append(';');
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      try { appendTo(sb); } catch(IOException e) { return ""; }
      return sb.toString();
    }

    public ServiceMetricsGroup addAttribute(String value) {
      attributeValues.add(value);
      return this;
    }

    public ServiceMetricsGroup addMetric(ServiceMetricsMetric metric) {
      metrics.add(metric);
      return this;
    }

    public ServiceMetricsGroup() {

    }

    public ServiceMetricsGroup(String line) {
      int delim = line.indexOf("=");

      String[] attributes;
      String[] metrics;

      if(delim < 0) {
        attributes = new String[0];
        metrics = line.split(",");
      } else {
        attributes = line.substring(0,delim).split(":");
        metrics = line.substring(delim+1).split(",");
      }

      for( String attribute : attributes )
        addAttribute(attribute);

      for( String metric : metrics )
        addMetric(new ServiceMetricsMetric(metric));
    }

    public boolean equals(Object obj) {
      if(!(obj instanceof ServiceMetricsGroup))
        return false;
      ServiceMetricsGroup group = (ServiceMetricsGroup)obj;

      return this.contains(group) && group.contains(this);
    }

    public boolean contains(ServiceMetricsGroup group) {
      for( String value : group.attributeValues ) {
        if(!attributeValues.contains(value))
          return false;
      }
      for( ServiceMetricsMetric metric : group.metrics ) {
        if(!metrics.contains(metric)) {
          return false;
        }
      }
      return true;
    }
  }

  // a line is a service metrics line if it contains more than one '=' symbol
  public static class ServiceMetricsLine extends QuerylogLine {
    protected final ArrayList<ServiceMetricsGroup> groups = new ArrayList<ServiceMetricsGroup>();

    public void appendTo(Appendable a) throws IOException {
      a.append(name);
      a.append('=');

      Iterator<ServiceMetricsGroup> iter = groups.iterator();
      while(iter.hasNext()) {
        ServiceMetricsGroup group = iter.next();
        group.appendTo(a);
      }
      a.append('\n');
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      try { appendTo(sb); } catch(IOException e) { return ""; }
      return sb.toString();
    }

    ServiceMetricsLine(int dummy, String line) {
      super(line);

      String[] groupEntries = rawValue.split(";");
      for( String group : groupEntries )
        addGroup(new ServiceMetricsGroup(group));
    }

    public ServiceMetricsLine(String name) {
      super(name,null);
    }

    public ServiceMetricsLine addGroup(ServiceMetricsGroup group) {
      groups.add(group);
      return this;
    }

    public boolean equals(Object obj) {
      if(!(obj instanceof ServiceMetricsLine))
        return false;
      ServiceMetricsLine line = (ServiceMetricsLine)obj;
      return name.equals(line.name) && this.contains(line) && line.contains(this);
    }

    public boolean contains(ServiceMetricsLine line) {
      for(ServiceMetricsGroup group : line.groups) {
        if(groups.contains(group) == false)
          return false;
      }
      return true;
    }
  }

  public static class QuerylogLine {
    protected String name;
    protected String rawValue;

    public QuerylogLine(String name, String rawValue) {
      this.name = name;
      this.rawValue = rawValue;
    }

    public void appendTo(Appendable a) throws IOException {
      a.append(name);
      a.append('=');
      a.append(rawValue);
      a.append('\n');
    }

    public String getName() {
      return name;
    }

    public String getRawValue() {
      return rawValue;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      try { appendTo(sb); } catch(IOException e) { return ""; }
      return sb.toString();
    }

    public QuerylogLine(String line) {
      int delim = line.indexOf("=");
      if(delim < 0)
        throw new IllegalArgumentException("Line missing '=' separator");
      name = line.substring(0,delim);
      rawValue = line.substring(delim+1);
    }

    public boolean equals(Object obj) {
      if(!(obj instanceof QuerylogLine))
        return false;
      QuerylogLine line = (QuerylogLine)obj;

      return name.equals(line.name) && rawValue.equals(line.rawValue);
    }
  }

  public static class QuerylogRecord {
    protected final ArrayList<QuerylogLine> lines = new ArrayList<QuerylogLine>();

    public void appendTo(Appendable a) throws IOException {
      a.append("------------------------------------------------------------------------\n");
      for(QuerylogLine line : lines)
        line.appendTo(a);
      a.append("EOE\n");
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      try { appendTo(sb); } catch(IOException e) { return ""; }
      return sb.toString();
    }

    QuerylogRecord(BufferedReader reader) throws IOException {
      // builds a record from an input stream
      String line;
      while(reader.ready() && (line = reader.readLine()) != null) {
        if(line.startsWith("EOE")) {
          break;
        } else if(line.indexOf("=") > 0 && !line.startsWith("Levels")) {
          if(isServiceMetricsLine(line))
            addLine(new ServiceMetricsLine(0,line));
          else
            addLine(new QuerylogLine(line));
        }
      }

    }

    public QuerylogRecord() {
      // creates an empty record
    }

    public QuerylogRecord addLine(QuerylogLine line) {
      lines.add(line);
      return this;
    }

    public boolean equals(Object obj) {
      if(!(obj instanceof QuerylogRecord))
        return false;

      QuerylogRecord record = (QuerylogRecord)obj;
      return this.contains(record) && record.contains(this);
    }

    public boolean contains(QuerylogRecord record) {
      Iterator<QuerylogLine> thatLines = record.lines.iterator();
      while(thatLines.hasNext()) {
        if(this.lines.contains(thatLines.next()) == false)
          return false;
      }

      return true;
    }
  }

  protected static boolean isServiceMetricsLine(String line) {
    // count equals signs, if more than one, return true
    int delim = line.indexOf("=");
    if(delim < 0)
      return false;
    if(line.indexOf("=",delim+1) >= 0 ||
       line.indexOf("#",delim+1) >= 0)
      return true;
    else
      return false;
  }
}
