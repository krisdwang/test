package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.locks.ReentrantLock;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;
import com.amazon.coral.metrics.NullLog;
import com.amazon.coral.metrics.*;
import java.util.TimeZone;

public class QuerylogReporterTest {

  private final QuerylogFormat format = new QuerylogFormat(new StaticCalendarFactory(TimeZone.getTimeZone("UTC")));

  private Group serviceMetricsGroup() {
    return new GroupBuilder("ServiceMetrics").newGroup();
  }

  private Group defaultGroup() {
    return new GroupBuilder().newGroup();
  }

  @Test(expected=IllegalArgumentException.class)
  public void garbageParameter() throws Throwable {
    new QuerylogReporter(null, new ReentrantLock(), format);
  }

  @Test(expected=IllegalArgumentException.class)
  public void garbageLog() throws Throwable {
    new QuerylogReporter(null, new StringWriter(), new ReentrantLock(), format, false);
  }

  @Test(expected=IllegalArgumentException.class)
  public void garbageFormat() throws Throwable {
    new QuerylogReporter(new StringWriter(), new ReentrantLock(), null);
  }

  @Test
  public void addTimes() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
        protected void addFormattedTimings(CharSequence c) throws IOException {
          s[0] = c;
        }
    };
    Group g = defaultGroup();

    r.beginReport();
    r.addTime(g,"name", 1.0, SI.SECOND, 1);
    r.addTime(g,"name", 1.0, SI.SECOND, 1);
    r.endReport();

    "name:1000/1,name:1000/1".contentEquals(s[0]);

  }

  @Test
  public void addLevels() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
        protected void addFormattedLevels(CharSequence c) throws IOException {
          s[0] = c;
        }
    };
    Group g = defaultGroup();

    r.beginReport();
    r.addLevel(g,"name", 1.0, SI.SECOND, 1);
    r.addLevel(g,"name", 1.0, SI.SECOND, 1);
    r.endReport();
    "name:1000/1,name:1000/1".contentEquals(s[0]);

  }

  @Test
  public void addTimeSeconds() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedTimings(CharSequence c) throws IOException {
        s[0] = c;
      }
    };
    Group g = defaultGroup();

    r.beginReport();
    r.addTime(g,"name", 1.0, SI.SECOND, 1);
    r.endReport();

    "name:1000/1".contentEquals(s[0]);

  }

  @Test
  public void addTimeMilliseconds() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedTimings(CharSequence c) throws IOException {
        s[0] = c;
      }
    };

    r.beginReport();
    r.addTime(defaultGroup(),"name", 1.0, SI.MILLI(SI.SECOND), 1);
    r.endReport();

    "name:1/1".contentEquals(s[0]);

  }

  @Test
  public void addTimeNanoseconds() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedTimings(CharSequence c) throws IOException {
        s[0] = c;
      }
    };
    Group g = defaultGroup();

    r.beginReport();
    r.addTime(g,"name", 1.0, SI.NANO(SI.SECOND), 1);
    r.endReport();

    "name:0.000001/1".contentEquals(s[0]);

  }

  @Test
  public void addTimeBadUnit() throws Throwable {

    final boolean failed[] = new boolean[] { false };
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void errorWriting(Throwable t) {
        failed[0] = true;
      }
    };
    Group g = defaultGroup();

    r.beginReport();
    r.addTime(g,"name", 1.0, SI.MOLE, 1);
    r.endReport();

    assertTrue(failed[0]);

  }

  @Test
  public void addCounts() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedCounters(CharSequence c) throws IOException {
        s[0] = c;
      }
    };
    Group g = defaultGroup();

    r.beginReport();
    r.addCount(g,"name", 1.0, Unit.ONE, 1);
    r.addCount(g,"name", 1.0, Unit.ONE, 1);
    r.endReport();

    "name=1,name=1".contentEquals(s[0]);

  }

  @Test
  public void addCount() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedCounters(CharSequence c) throws IOException {
        s[0] = c;
      }
    };
    Group g = defaultGroup();

    r.beginReport();
    r.addCount(g,"name", 1.0, Unit.ONE, 1);
    r.endReport();

    "name=1".contentEquals(s[0]);

  }

  @Test
  public void addCountWithUnit() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedCounters(CharSequence c) throws IOException {
        s[0] = c;
      }
    };
    Group g = defaultGroup();

    r.beginReport();
    r.addCount(g,"name", 1.0, SI.MOLE, 1);
    r.endReport();

    "name=1 mol".contentEquals(s[0]);

  }

  @Test
  public void programProperty() throws Throwable {
    final CharSequence[] s = new CharSequence[1];

    QuerylogFormat f = new QuerylogFormat(new StaticCalendarFactory(TimeZone.getTimeZone("UTC"))) {
      @Override
      public void writeProperty(StringBuilder sb, String name, CharSequence value) throws IOException {
        if("Program".equals(name)) {
          if(s[0] != null)
            fail();

          s[0] = value;
        }
      }
    };

    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), f);
    Group g = defaultGroup();

    r.beginReport();
    r.addProperty(g, "Program", "FooProgram");
    r.endReport();

    assertEquals("FooProgram", s[0]);
  }

  @Test
  public void serviceMappedToProgram() throws Throwable {
    final CharSequence[] s = new CharSequence[1];

    QuerylogFormat f = new QuerylogFormat(new StaticCalendarFactory(TimeZone.getTimeZone("UTC"))) {
      @Override
      public void writeProperty(StringBuilder sb, String name, CharSequence value) throws IOException {
        if("Program".equals(name)) {
          if(s[0] != null)
            fail();

          s[0] = value;
        }
      }
    };

    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), f);
    Group g = defaultGroup();

    r.beginReport();
    r.addProperty(g, "Service", "FooProgram");
    r.endReport();

    assertEquals("FooProgram", s[0]);
  }

  @Test
  public void serviceOverwritesProgram() throws Throwable {
    final CharSequence[] s = new CharSequence[1];

    QuerylogFormat f = new QuerylogFormat(new StaticCalendarFactory(TimeZone.getTimeZone("UTC"))) {
      @Override
      public void writeProperty(StringBuilder sb, String name, CharSequence value) throws IOException {
        if("Program".equals(name)) {
          if(s[0] != null)
            fail();

          s[0] = value;
        }
      }
    };

    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), f);
    Group g = defaultGroup();

    r.beginReport();
    r.addProperty(g, "Service", "FooProgram");
    r.addProperty(g, "Program", "OverwriteMe");
    r.endReport();

    assertEquals("FooProgram", s[0]);
  }


  @Test
  public void addSizeCount() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addCount(g,"Size", 1.0, Unit.ONE, 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nSize=1\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addBatchSizeCount() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addCount(g,"BatchSize", 1.0, Unit.ONE, 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nBatchSize=1\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addErrorRateLevel() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addLevel(g,"ErrorRate", 1.0, Unit.ONE, 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nErrorRate=1\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addLevel() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addLevel(g,"Level", 1.0, Unit.ONE, 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nOperation=Unknown\nLevels=Level=1/1\nEOE\n", w.toString());

  }

  @Test
  public void addLevelDecimal() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addLevel(g,"Level", 1.78, Unit.ONE, 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nOperation=Unknown\nLevels=Level=1.78/1\nEOE\n", w.toString());

  }

  @Test
  public void addISODate() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addDate(g,"isoDate", 1.0);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nisoDate=1970-01-01T00:00:00+0000\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addStartTimeDate() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addDate(g,"StartTime", 1.0);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nStartTime=0.001\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addEndTimeDate() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addDate(g,"EndTime", 1.0);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nEndTime=Thu, 01 Jan 1970 00:00:00 UTC\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addUnsupportedDateToServiceMetrics() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = serviceMetricsGroup();

    r.beginReport();
    r.addDate(g,"Unsupported", 1.0);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addTimeLiterally() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addTime(g,"Time", 1.0, SI.MILLI(SI.SECOND), 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nTime=1 ms\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addUserTime() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addTime(g,"UserTime", 1.0, SI.MILLI(SI.SECOND), 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nUserTime=1 ms\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void addSystemTime() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = defaultGroup();

    r.beginReport();
    r.addTime(g,"SystemTime", 1.0, SI.MILLI(SI.SECOND), 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nSystemTime=1 ms\nOperation=Unknown\nEOE\n", w.toString());

  }

  @Test
  public void conflictsLogged() throws Throwable {
    final boolean logged[] = { false };
    NullLog log = new NullLog() {
      @Override
      public boolean isDebugEnabled() { return true; }
      @Override
      public void debug(Object message) {
        logged[0] = true;
      }
    };
    Group g = defaultGroup();
    QuerylogReporter r = new QuerylogReporter(log, new StringWriter(), new ReentrantLock(), format, false);
    r.beginReport();
    r.addTime(g,"FOO", 1.0, SI.MILLI(SI.SECOND), 1);
    r.addTime(g,"FOO", 1.0, SI.MILLI(SI.SECOND), 1);
    assertTrue(logged[0]);
  }

  @Test
  public void errorsLogged() throws Throwable {
    final boolean logged[] = { false };
    NullLog log = new NullLog() {
      @Override
      public void error(Object message, Throwable cause) {
        logged[0] = true;
      }
    };
    new QuerylogReporter(log, new StringWriter(), new ReentrantLock(), format, false).errorWriting(new Error());
    assertTrue(logged[0]);
  }

  @Test
  public void addFormattedCounters() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    r.addFormattedCounters("foo");

    assertEquals("Counters=foo\n", w.toString());

  }

  @Test
  public void addFormattedLevels() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    r.addFormattedLevels("foo");

    assertEquals("Levels=foo\n", w.toString());

  }

  @Test
  public void addFormattedTimings() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    r.addFormattedTimings("foo");

    assertEquals("Timing=foo\n", w.toString());

  }

  @Test
  public void addFormattedServiceMetrics() throws Throwable {

    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);

    ServiceMetricsBuilder smb = new ServiceMetricsBuilder();
    Group g = new GroupBuilder("ServiceMetrics").newGroup();
    smb.addCount(g, "a", 1.0, Unit.ONE, 1);
    r.addFormattedServiceMetrics(smb);

    assertEquals("ServiceMetrics=a#C/1/1/;\n", w.toString());

  }

}
