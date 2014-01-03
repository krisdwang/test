package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.GroupBuilder;
import java.util.concurrent.locks.Lock;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.Writer;
import javax.measure.unit.Unit;
import javax.measure.unit.SI;
import javax.measure.unit.NonSI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Eric Crahen <crahen@amazon.com>
 */
class QuerylogReporter implements Reporter {

  // will come back later and make a predefined Group.DEFAULT object w/ fast-comparison
  private final static Group DEFAULT_GROUP = new GroupBuilder().newGroup();

  private final static Log DEFAULT_LOG = LogFactory.getLog(QuerylogReporter.class);
  private final QuerylogFormat format;
  private final Log log;
  private final Writer writer;
  private final Lock writerLock;
  private final boolean optimisticSampling;

  private ServiceMetricsBuilder serviceMetrics = null;
  private StringBuilder levels = null;
  private StringBuilder counters = null;
  private StringBuilder timers = null;
  private StringBuilder defaultEntries = null;
  private boolean hasOperation = false;
  private CharSequence program = null;
  private CharSequence service = null;
  private Map<String,CharSequence> metrics = null;

  QuerylogReporter(Writer writer, Lock writerLock, QuerylogFormat format) {
    this(writer, writerLock, format, false);
  }

  public QuerylogReporter(Writer writer, Lock writerLock, QuerylogFormat format, boolean optimisticSampling) {
    this(DEFAULT_LOG, writer, writerLock, format, optimisticSampling);
  }

  QuerylogReporter(Log log, Writer writer, Lock writerLock, QuerylogFormat format, boolean optimisticSampling) {
    if(log == null)
      throw new IllegalArgumentException();
    if(format == null)
      throw new IllegalArgumentException();
    if(writer == null)
      throw new IllegalArgumentException();
    if(writerLock == null)
      throw new IllegalArgumentException();
    this.log = log;
    this.format = format;
    this.writer = writer;
    this.writerLock = writerLock;
    this.optimisticSampling = optimisticSampling;
  }

  private boolean validate() {
    return log.isDebugEnabled();
  }

  /**
   */
  public void validateMetric(String metricName, CharSequence metricType) {
    if(validate()) {
      CharSequence otherType = metrics.put(metricName, metricType);
      if(otherType != null)
        log.debug("CONFLICT detected: PMET will not aggregate both " +
                 metricType + ":" + metricName  +
                 " and " +
                 otherType + ":" + metricName);
    }
  }

  /**
   */
  public void beginReport() {
    if(validate())
      metrics = new HashMap<String,CharSequence>();
    defaultEntries = new StringBuilder();
    defaultEntries.append("------------------------------------------------------------------------\n");
    serviceMetrics = null;
    counters = null;
    timers = null;
    levels = null;
  }

  /**
   */
  public void addProperty(Group group, String name, CharSequence value) {

    if(DEFAULT_GROUP.equals(group)) {
      if("Operation".equals(name))
        hasOperation = true;

      if("Program".equals(name)) {
        program = value;
        return;
      }

      if("Service".equals(name)) {
        service = value;
        return;
      }

      try {
        format.writeProperty(defaultEntries, name, value);
      } catch(Throwable t) { errorWriting(t); }
      validateMetric(name, "Property");
    }
    // ServiceMetrics doesn't support properties, so discard if not default group
  }

  /**
   */
  public void addDate(Group group, String name, double value) {

    if(DEFAULT_GROUP.equals(group)) {
      try {
        // format object handles special formatting for StartTime, EndTime
        format.writeDefaultDate(defaultEntries, name, value);
      } catch(Throwable t) { errorWriting(t); }
      validateMetric(name, "Date");
    }
    // service metrics ignores any date fields, so discard if not default group
  }

  /**
   */
  public void addCount(Group group, String name, double value, Unit<?> unit, int count) {

    try {

      if(DEFAULT_GROUP.equals(group)) {

        // The "Size" and "BatchSize" are two legacy types that have a special
        // formatting
        if("Size".equals(name) || "BatchSize".equals(name))
          format.writeDefaultCount(defaultEntries, name, value, unit, count);

        else {
          if(counters == null)
            counters = new StringBuilder();
          else
            counters.append(',');
          counters.append(name).append('=');
          format.appendValue(counters,value);
          if(!Unit.ONE.equals(unit))
            counters.append(' ').append(unit.toString());
        }

        validateMetric(name, "Count");

      } else {

        getServiceMetricsBuilder().addCount(group, name, value, unit, count);

      }

    } catch(Throwable t) { errorWriting(t); }

  }

  /**
   */
  public void addTime(Group group, String name, double value, Unit<?> unit, int count) {

    try {

      if(DEFAULT_GROUP.equals(group)) {

        // The "Time", "UserTime" and "SystemTime" are legacy types that have a special
        // formatting
        if("Time".equals(name) || "UserTime".equals(name) || "SystemTime".equals(name))
          format.writeDefaultTime(defaultEntries, name, value, unit, count);

        else {
          if(timers == null)
            timers = new StringBuilder();
          else
            timers.append(',');
          timers.append(name).append(':');

          value = format.toMilliseconds(value, unit);
          format.appendValue(timers,value);
          timers.append('/').append(count);

        }

        validateMetric(name, "Time");

      } else {

        // Remap Time in a ServiceMetrics group to Latency (frameworks like
        // CoralOrchestrator just measure Time)
        boolean isServiceMetrics = "ServiceMetrics".equals(group.getName());
        if(isServiceMetrics && "Time".equals(name))
          getServiceMetricsBuilder().addLevel(group, "Latency", value, unit, count);
        else
          getServiceMetricsBuilder().addTime(group, name, value, unit, count);

      }

    } catch(Throwable t) { errorWriting(t); }

  }

  /**
   */
  public void addLevel(Group group, String name, double value, Unit<?> unit, int count) {

    try {

      if(DEFAULT_GROUP.equals(group)) {

        // The "ErrorRate" is a legacy type that has a special formatting
        if("ErrorRate".equals(name))
          format.writeDefaultLevel(defaultEntries, name, value, unit, count);

        else {
          if(levels == null)
            levels = new StringBuilder();
          else
            levels.append(',');
          levels.append(name).append('=');

          format.appendValue(levels,value);
          levels.append('/').append(count);

          if(!Unit.ONE.equals(unit)) {
            levels.append(' ');
            // https://devcentral.amazon.com/contact-us/ContactUsIssue.cgi?issue=1462&profile=platform-libraries
            // second:       's'
            // byte:         'B'
            // millisecond:  'ms'
            // microsecond:  'us'
            // nanosecond:   'ns'
            // minute:       'min'
            // hour:         'h'
            // day:          'd'
            // megabyte:     'MB'
            // kilobyte:     'kB'
            // terabyte:     'TB'
            if(unit.equals(SI.MICRO(SI.SECOND)))
              levels.append("us");
            else
            if(unit.equals(NonSI.DAY))
              levels.append("d");
            else
            if(unit.equals(NonSI.BYTE))
              levels.append("B");
            else
            if(unit.equals(SI.KILO(NonSI.BYTE)))
              levels.append("kB");
            else
            if(unit.equals(SI.MEGA(NonSI.BYTE)))
              levels.append("MB");
            else
            if(unit.equals(SI.TERA(NonSI.BYTE)))
              levels.append("TB");
            else
              levels.append(unit.toString());

          }

          validateMetric(name, "Level");

        }

      } else {

        getServiceMetricsBuilder().addLevel(group, name, value, unit, count);

      }
    } catch(Throwable t) { errorWriting(t); }

  }

  /**
   */
  public void endReport() {

    // If no Operation metric was provided, emit Unknown
    if(!hasOperation)
      try {
        format.writeProperty(defaultEntries, "Operation", "Unknown");
      } catch(Throwable t) { errorWriting(t); }

    // overwrite "Program" property with "Service" property
    if(service != null)
      program = service;

    if(program != null)
      try {
        format.writeProperty(defaultEntries, "Program", program);
      } catch(Throwable t) { errorWriting(t); }

    try {

      if(!optimisticSampling || writerLock.tryLock()) {

        if(!optimisticSampling)
          writerLock.lock();

        try {
          writer.append(defaultEntries);

          if(serviceMetrics != null) {
            addFormattedServiceMetrics(serviceMetrics);
          }

          if(timers != null)
            addFormattedTimings(timers);

          if(counters != null)
            addFormattedCounters(counters);

          if(levels != null)
            addFormattedLevels(levels);

          // release memory
          serviceMetrics = null;
          counters = null;
          timers = null;
          levels = null;
          defaultEntries = null;

          writer.append("EOE\n").flush();

        } finally {
          writerLock.unlock();
        }

      }

    } catch(Throwable t) {
      errorWriting(t);
    }
  }

  /**
   */
  protected void addFormattedServiceMetrics(ServiceMetricsBuilder smb) throws IOException {
    smb.appendTo(writer);
  }

  /**
   */
  protected void addFormattedLevels(CharSequence s) throws IOException {
    writer.append("Levels=").append(s).append("\n");
  }

  /**
   */
  protected void addFormattedCounters(CharSequence s) throws IOException {
    writer.append("Counters=").append(s).append("\n");
  }

  /**
   */
  protected void addFormattedTimings(CharSequence s) throws IOException {
    writer.append("Timing=").append(s).append("\n");
  }

  /**
   */
  protected void errorWriting(Throwable t) {
    log.error("Failed to write querylog", t);
  }

  private ServiceMetricsBuilder getServiceMetricsBuilder() throws IOException {
    if(serviceMetrics == null)
      serviceMetrics = new ServiceMetricsBuilder();
    return serviceMetrics;
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
