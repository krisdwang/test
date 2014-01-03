package com.amazon.coral.metrics.reporter;

import java.io.IOException;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;
import javax.measure.converter.UnitConverter;

/**
 * @author Eric Crahen <crahen@amazon.com>
 */
class QuerylogFormat {

  private final static Unit<?> MILLISECOND = SI.MILLI(SI.SECOND);
  private final static Unit<?> NANOSECOND = SI.NANO(SI.SECOND);
  private final static double NANO_TO_MILLI = 1.0 / 1000000.0;

  private final CalendarFactory calendarFactory;

  private final DateFormat endTimeFormat = new CachedDateFormat(new EndTimeFormat());
  private final DateFormat isoDateFormat = new CachedDateFormat(new ISO8601Format());

  // This DoubleFormat is used in formatting the StartTime.
  private final DoubleFormat numberDateFormat = new DoubleFormat();
  // This DoubleFormat is used in formatting times.
  private final DoubleFormat numberTimeFormat = new DoubleFormat();

  QuerylogFormat(CalendarFactory calendarFactory) {
    if(calendarFactory == null)
      throw new IllegalArgumentException();
    this.calendarFactory = calendarFactory;
    numberDateFormat.setMaximumFractionDigits(3);
    numberDateFormat.setMinimumFractionDigits(3);
    numberTimeFormat.setMaximumFractionDigits(6);
  }

  /**
   * Write a Property that is a part of the default group.
   */
  public void writeProperty(StringBuilder sb, String name, CharSequence value)
    throws IOException {

    sb.append(name).append("=");
    escapeNewLines(sb, value);
    sb.append("\n");
  }

  /**
   * Append the given charsequence while escaping newlines.  This is about an
   * order of magnitude faster than the original variant that used
   * String.replace().
   */
  protected void escapeNewLines(StringBuilder destination, CharSequence cs) {
    if(cs == null)
      return;

    int length = cs.length();
    for(int i=0; i < length; ++i) {
      char c = cs.charAt(i);

      if(c == '\n') {
        destination.append("\\n");
        continue;
      }
      if(c == '\r') {
        destination.append("\\r");
        continue;
      }

      destination.append(c);
    }
  }

  /**
   * Write a Date that is a part of the default group.
   */
  public void writeDefaultDate(StringBuilder sb, String name, double value)
    throws IOException {

    sb.append(name).append("=");

    // There are two Date metrics which have a special format in the Querylog
    // format
    if("StartTime".equals(name)) {
      // StartTime in seconds-since-epoch
      // The value provided is in milliseconds-since-epoch, convert to seconds (/1000)
      numberDateFormat.format(sb, value / 1000.0);
    } else
    if("EndTime".equals(name)) {
      // EndTime in human readable form
      endTimeFormat.format(sb, value, calendarFactory);
    } else {
      isoDateFormat.format(sb, value, calendarFactory);
    }

    sb.append("\n");

  }

  // For Time, SystemTime, UserTime
  public void writeDefaultTime(StringBuilder sb, String name, double value, Unit<?> unit, int repeat)
    throws IOException {

    sb.append(name).append("=");
    value = toMilliseconds(value, unit);
    numberTimeFormat.format(sb, value);
    sb.append(" ms");
    sb.append("\n");

  }

  // For Size, BatchSize
  public void writeDefaultCount(StringBuilder sb, String name, double value, Unit<?> unit, int repeat)
    throws IOException {

    sb.append(name).append("=");
    numberTimeFormat.format(sb, value);
    sb.append("\n");

  }

  // For ErrorRate
  public void writeDefaultLevel(StringBuilder sb, String name, double value, Unit<?> unit, int repeat)
    throws IOException {

    sb.append(name).append("=");
    numberTimeFormat.format(sb, value);
    sb.append("\n");

  }

  public double toMilliseconds(double value, Unit<?> unit) {

    // Check the Unit and do a fast check for SECOND, MILLISECONDS or
    // NANOSECONDS with the standard Unit classes and convert whatever
    // it is we have into MILLISECONDS
    if(!MILLISECOND.equals(unit)) {
      if(SI.SECOND.equals(unit)) {
        value *= 1000;
      } else if(NANOSECOND.equals(unit)) {
        value *= NANO_TO_MILLI;
      } else {
        UnitConverter converter = unit.getConverterTo(MILLISECOND);
        value = converter.convert(value);
      }
    }

    return value;

  }

  public void appendValue(StringBuilder builder, double value) {
    numberTimeFormat.format(builder, value);
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
