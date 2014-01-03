package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;
import javax.measure.unit.Unit;
import javax.measure.unit.SI;
import java.util.TimeZone;

public class QuerylogFormatTest {

  private final static TimeZone timeZone = TimeZone.getTimeZone("UTC");
  private final QuerylogFormat format = new QuerylogFormat(new StaticCalendarFactory(timeZone));

  @Test
  public void escapeNewlines() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.escapeNewLines(w, (CharSequence)"\n");
    assertEquals("\\n", w.toString());
  }

  @Test
  public void escapeNewlinesNull() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.escapeNewLines(w, null);
    assertEquals("", w.toString());
  }

  @Test
  public void writeProperty() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeProperty(w, "Foo", "Bar");
    assertEquals("Foo=Bar\n", w.toString());
  }

  @Test
  public void writePropertyWithNewLineValue() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeProperty(w, "Foo", "Bar\n");
    assertEquals("Foo=Bar\\n\n", w.toString());
  }

  @Test
  public void writePropertyWithCarridgeReturnValue() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeProperty(w, "Foo", "Bar\r");
    assertEquals("Foo=Bar\\r\n", w.toString());
  }

  @Test
  public void writeDateEndTime() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeDefaultDate(w, "EndTime", 400000000);
    assertEquals("EndTime=Mon, 05 Jan 1970 15:06:40 UTC\n", w.toString());
  }

  @Test
  public void writeDateStartTime() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeDefaultDate(w, "StartTime", 400000000);
    assertEquals("StartTime=400000.000\n", w.toString());
  }

  @Test
  public void writeDate() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeDefaultDate(w, "AnyDate", 400000000);
    assertEquals("AnyDate=1970-01-05T15:06:40+0000\n", w.toString());
  }

  @Test
  public void writeTimeSeconds() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeDefaultTime(w, "AnyTime", 40.1, SI.SECOND, 1);
    assertEquals("AnyTime=40100 ms\n", w.toString());
  }

  @Test
  public void writeTimeMilliseconds() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeDefaultTime(w, "AnyTime", 40.1, SI.MILLI(SI.SECOND), 1);
    assertEquals("AnyTime=40.1 ms\n", w.toString());
  }

  @Test
  public void writeTimeNanoseconds() throws Throwable {
    StringBuilder w = new StringBuilder();
    format.writeDefaultTime(w, "AnyTime", 40100.0, SI.NANO(SI.SECOND), 1);
    assertEquals("AnyTime=0.0401 ms\n", w.toString());
  }

  @Test
  public void appendValue() throws Throwable {
    StringBuilder b = new StringBuilder();
    format.appendValue(b, 1);
    assertEquals("1", b.toString());
  }

  @Test
  public void appendValueWithFraction() throws Throwable {
    StringBuilder b = new StringBuilder();
    format.appendValue(b, 1.12345);
    assertEquals("1.12345", b.toString());
  }

  @Test
  public void appendValueWithFractionRounded() throws Throwable {
    StringBuilder b = new StringBuilder();
    format.appendValue(b, 1.1234567);
    assertEquals("1.123457", b.toString());
  }

  @Test
  public void conversionToMilliseconds() throws Throwable {
    Unit<?> unit = SI.MICRO(SI.SECOND);
    assertEquals(.001, format.toMilliseconds(1, unit), 0);
  }

}
