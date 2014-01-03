package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.*;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import java.io.StringWriter;
import javax.measure.unit.SI;
import javax.measure.unit.NonSI;
import javax.measure.unit.Unit;
import java.util.TimeZone;

public class QuerylogAcceptanceTest {

  Reporter newQuerylogReporter(StringWriter w) {
    return new QuerylogReporterFactory(w, TimeZone.getTimeZone("UTC")).newReporter();
  }

  Reporter newQuerylogReporter(StringWriter w, TimeZone timeZone) {
    return new QuerylogReporterFactory(w, timeZone).newReporter();
  }

  private Group serviceMetricsGroup(String service,String operation) {
    GroupBuilder gb = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName",service);
    if(operation != null)
      gb.setAttribute("Operation",operation);

    return gb.newGroup();
  }

  private Group defaultGroup() {
    return new GroupBuilder().newGroup();
  }

  @Test(expected=IllegalArgumentException.class)
  public void configuredWrong() throws Throwable {
    newQuerylogReporter(null);
  }

  // Smallest thing we can output
  @Test
  public void minimalRecord() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    r.beginReport();
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  // Smallest thing we can output that PMET understands
  @Test
  public void minimalSaneRecord() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = defaultGroup();
    r.beginReport();
    r.addProperty(g,"Marketplace", "USAmazon");
    r.addProperty(g,"ServiceName", "Service");
    r.addProperty(g,"Operation", "Operation");
    r.addDate(g,"StartTime", 400000000); // milliseconds
    r.addDate(g,"EndTime", 400000000);
    r.addTime(g,"Time", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Marketplace=USAmazon\n" +
      "ServiceName=Service\n" +
      "Operation=Operation\n" +
      "StartTime=400000.000\n" + // seconds
      "EndTime=Mon, 05 Jan 1970 15:06:40 UTC\n" +
      "Time=0 ms\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void minimalSaneRecordPST() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w, TimeZone.getTimeZone("PST"));
    Group g = defaultGroup();
    r.beginReport();
    r.addProperty(g,"Marketplace", "USAmazon");
    r.addProperty(g,"ServiceName", "Service");
    r.addProperty(g,"Operation", "Operation");
    r.addDate(g,"StartTime", 400000000); // milliseconds
    r.addDate(g,"EndTime", 400000000);
    r.addTime(g,"Time", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Marketplace=USAmazon\n" +
      "ServiceName=Service\n" +
      "Operation=Operation\n" +
      "StartTime=400000.000\n" + // seconds
      "EndTime=Mon, 05 Jan 1970 07:06:40 PST\n" +
      "Time=0 ms\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricsTime() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service","Operation");

    r.beginReport();
    r.addTime(g,"Time0", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Time0#T/0/1/ms;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricsCount() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service","Operation");
    r.beginReport();
    r.addCount(g,"Count", 0, Unit.ONE, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Count#C/0/1/;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricsLevel() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service","Operation");
    r.beginReport();
    r.addLevel(g,"Level", 0, Unit.ONE, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Level#L/0/1/;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricsLevelCount() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service","Operation");
    r.beginReport();
    r.addCount(g,"Count", 0, Unit.ONE, 1);
    r.addLevel(g,"Level", 0, Unit.ONE, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Count#C/0/1/,Level#L/0/1/;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricsTimeCount() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service","Operation");
    r.beginReport();
    r.addTime(g,"Time0", 0, SI.SECOND, 1);
    r.addCount(g,"Count", 0, Unit.ONE, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Time0#T/0/1/ms,Count#C/0/1/;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricsLevelTime() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service","Operation");
    r.beginReport();
    r.addTime(g,"Time0", 0, SI.SECOND, 1);
    r.addLevel(g,"Level", 0, Unit.ONE, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Time0#T/0/1/ms,Level#L/0/1/;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricRemoteTime() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service","Operation");
    r.beginReport();
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Remote#L/0/1/ms,Remote1#T/0/1/ms;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricMultipleRemoteTime() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service","Operation");
    r.beginReport();
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Remote#L/0/3/ms,Remote1#T/0/1/ms;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricMultipleRemoteTimeMultipleService() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g;
    g = serviceMetricsGroup("Service","Operation");
    r.beginReport();
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    g = serviceMetricsGroup("Service2","Operation");
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service:Operation=Remote#L/0/3/ms,Remote1#T/0/1/ms;Service2:Operation=Remote#L/0/3/ms,Remote1#T/0/1/ms;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricsServiceOnly() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g = serviceMetricsGroup("Service", null);
    r.beginReport();
    r.addTime(g,"Time", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service=Latency#L/0/1/ms;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void serviceMetricsServiceOnlyMany() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);
    Group g1 = serviceMetricsGroup("Service1", null);
    Group g2 = serviceMetricsGroup("Service2", null);
    r.beginReport();
    r.addTime(g1,"Time", 0, SI.SECOND, 1);

    r.addTime(g2,"Time", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "ServiceMetrics=Service1=Latency#L/0/1/ms;Service2=Latency#L/0/1/ms;\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void levels() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();
    r.beginReport();
    r.addLevel(g,"Remote", 0, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=Remote=0/1 s\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void levelSuccessRate() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    // 9 of out 10 successes (Unit.ONE's)
    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"SuccessRate", 0, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.addLevel(g,"SuccessRate", 1, Unit.ONE, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=SuccessRate=0.9/10\n" +
      "EOE\n";

    assertEquals(new QuerylogDOM(expected), new QuerylogDOM(w.toString()));

  }

  @Test
  public void levelSeconds() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, SI.SECOND, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 s\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelMilliseconds() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, SI.MILLI(SI.SECOND), 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 ms\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }


  @Test
  public void levelMicroseconds() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, SI.MICRO(SI.SECOND), 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 us\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelNanoseconds() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, SI.NANO(SI.SECOND), 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 ns\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelDays() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, NonSI.DAY, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 d\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelHours() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, NonSI.HOUR, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 h\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelMinutes() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, NonSI.MINUTE, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 min\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelByte() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, NonSI.BYTE, 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 B\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelMegabyte() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, SI.MEGA(NonSI.BYTE), 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 MB\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelTerabyte() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, SI.TERA(NonSI.BYTE), 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 TB\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }

  @Test
  public void levelKilobyte() throws Throwable {

    StringWriter w = new StringWriter();
    Reporter r = newQuerylogReporter(w);

    Group g = new GroupBuilder().newGroup();

    r.beginReport();
    r.addLevel(g,"X", 1, SI.KILO(NonSI.BYTE), 1);
    r.endReport();

    String expected =
      "------------------------------------------------------------------------\n" +
      "Operation=Unknown\n" +
      "Levels=X=1/1 kB\n" +
      "EOE\n";

    assertEquals(expected, w.toString());

  }


}
