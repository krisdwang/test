package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.*;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.locks.ReentrantLock;
import javax.measure.unit.SI;
import javax.measure.unit.NonSI;
import javax.measure.unit.Unit;
import java.util.TimeZone;

public class QuerylogServiceMetricsTest {

  private final QuerylogFormat format = new QuerylogFormat(new StaticCalendarFactory(TimeZone.getTimeZone("UTC")));
  private final Group GROUP = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","Service").newGroup();

  @Test
  public void addTime() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedServiceMetrics(ServiceMetricsBuilder smb) throws IOException {
        StringWriter sw = new StringWriter();
        smb.appendTo(sw);
        s[0] = sw.toString();
      }
    };

    r.beginReport();
    r.addTime(GROUP,"name", 1.0, SI.SECOND, 1);
    r.endReport();

    assertEquals("ServiceMetrics=Service=name#T/1000/1/ms;\n", s[0]);

  }

  @Test
  public void addTimes() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedServiceMetrics(ServiceMetricsBuilder smb) throws IOException {
        StringWriter sw = new StringWriter();
        smb.appendTo(sw);
        s[0] = sw.toString();
      }
    };

    r.beginReport();
    r.addTime(GROUP,"name", 1.0, SI.SECOND, 1);
    r.addTime(GROUP,"name", 1.0, SI.SECOND, 1);
    r.endReport();

    assertEquals("ServiceMetrics=Service=name#T/1000/1/ms,name#T/1000/1/ms;\n", s[0]);

  }

  @Test
  public void addCount() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedServiceMetrics(ServiceMetricsBuilder smb) throws IOException {
        StringWriter sw = new StringWriter();
        smb.appendTo(sw);
        s[0] = sw.toString();
      }
    };

    r.beginReport();
    r.addCount(GROUP,"name", 1.0, SI.MOLE, 1);
    r.endReport();

    assertEquals("ServiceMetrics=Service=name#C/1/1/mol;\n", s[0]);

  }

  @Test
  public void addCounts() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedServiceMetrics(ServiceMetricsBuilder smb) throws IOException {
        StringWriter sw = new StringWriter();
        smb.appendTo(sw);
        s[0] = sw.toString();
      }
    };

    r.beginReport();
    r.addProperty(GROUP,"ServiceName", "Service");
    r.addCount(GROUP,"name", 1.0, SI.MOLE, 1);
    r.addCount(GROUP,"name", 1.0, SI.MOLE, 1);
    r.endReport();

    assertEquals("ServiceMetrics=Service=name#C/1/1/mol,name#C/1/1/mol;\n", s[0]);

  }

  @Test
  public void addLevel() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedServiceMetrics(ServiceMetricsBuilder smb) throws IOException {
        StringWriter sw = new StringWriter();
        smb.appendTo(sw);
        s[0] = sw.toString();
      }
    };

    r.beginReport();
    r.addLevel(GROUP,"name", 1.0, NonSI.PERCENT, 1);
    r.endReport();

    assertEquals("ServiceMetrics=Service=name#L/1/1/%;\n", s[0]);

  }

  @Test
  public void addLevels() throws Throwable {

    final CharSequence[] s = new CharSequence[1];
    QuerylogReporter r = new QuerylogReporter(new StringWriter(), new ReentrantLock(), format) {
      @Override
      protected void addFormattedServiceMetrics(ServiceMetricsBuilder smb) throws IOException {
        StringWriter sw = new StringWriter();
        smb.appendTo(sw);
        s[0] = sw.toString();
      }
    };

    r.beginReport();
    r.addLevel(GROUP,"name", 1.0, NonSI.PERCENT, 1);
    r.addLevel(GROUP,"name", 1.0, NonSI.PERCENT, 1);
    r.endReport();

    assertEquals("ServiceMetrics=Service=name#L/1/1/%,name#L/1/1/%;\n", s[0]);

  }

  @Test
  public void verboseGroup() {
    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group g = new GroupBuilder("verbose").newGroup();

    r.beginReport();
    r.addCount(g, "a", 1, Unit.ONE, 1);
    r.addTime(g, "b", 1, SI.SECOND, 1);
    r.addLevel(g, "c", 1, NonSI.PERCENT, 1);

    // properties and dates not supported for ServiceMetrics (anything but default)
    // should have no output
    r.addProperty(g, "d", "abcdefg");
    r.addDate(g, "e", 1.0);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nOperation=Unknown\nverbose=a#C/1/1/,b#T/1000/1/ms,c#L/1/1/%;\nEOE\n", w.toString());
  }

  @Test
  public void serviceMetricsAndVerboseGroups() {
    StringWriter w = new StringWriter();
    QuerylogReporter r = new QuerylogReporter(w, new ReentrantLock(), format);
    Group groupSM = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","service").setAttribute("Operation","operation").setAttribute("attr3","attr3value").newGroup();
    Group groupV = new GroupBuilder("Verbose").newGroup();

    r.beginReport();
    r.addCount(groupSM, "serviceMetric", 1, Unit.ONE, 1);
    r.addCount(groupV, "verboseMetric", 2, Unit.ONE, 1);
    r.endReport();

    assertEquals("------------------------------------------------------------------------\nOperation=Unknown\nServiceMetrics=service:operation:attr3value=serviceMetric#C/1/1/;\nVerbose=verboseMetric#C/2/1/;\nEOE\n", w.toString());
  }

}
