package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.*;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.StringWriter;
import javax.measure.unit.*;

public class ServiceMetricsFormatTest {

  private final ServiceMetricsFormat format = new ServiceMetricsFormat();

  @Test
  public void writeUnitMS() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeUnit(w, SI.MILLI(SI.SECOND));
    assertEquals("ms", w.getBuffer().toString());
  }

  @Test
  public void writeUnitPercent() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeUnit(w, NonSI.PERCENT);
    assertEquals("%", w.getBuffer().toString());
  }

  @Test
  public void writeUnitNone() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeUnit(w, Unit.ONE);
    assertEquals("", w.getBuffer().toString());
  }

  @Test
  public void toMillisecondsFromSecond() {
    assertEquals(1010.10, format.toMilliseconds(1.01010, SI.SECOND), 0);
  }

  @Test
  public void toMillisecondsFromNano() {
    assertEquals(1.01010, format.toMilliseconds(1010100.0, SI.NANO(SI.SECOND)), 0);
  }

  @Test
  public void toMillisecondsConverter() {
    assertEquals(1010.10, format.toMilliseconds(1010100.0, SI.MICRO(SI.SECOND)), 0);
  }

  @Test
  public void writeSample() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeSample(w, "sample", null, 'C', 1.0, Unit.ONE, 1);
    assertEquals("sample#C/1/1/", w.getBuffer().toString());
  }

  @Test
  public void writeSampleCriticality() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeSample(w, "sample", "critical", 'C', 1.0, Unit.ONE, 1);
    assertEquals("sample:critical#C/1/1/", w.getBuffer().toString());
  }

  @Test
  public void writeSampleUnit() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeSample(w, "sample", null, 'T', 0.123, SI.MILLI(SI.SECOND), 1);
    assertEquals("sample#T/0.123/1/ms", w.getBuffer().toString());
  }

  @Test
  public void writeSampleTruncateValue() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeSample(w, "sample", null, 'T', 0.1234567, Unit.ONE, 1);
    assertEquals("sample#T/0.123/1/", w.getBuffer().toString());
  }

  @Test
  public void writeSampleCritical() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeSample(w, "sample", "Critical", 'T', 0.1234567, Unit.ONE, 1);
    assertEquals("sample:Critical#T/0.123/1/", w.getBuffer().toString());
  }

  @Test
  public void writeSampleCriticalEmptyString() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeSample(w, "sample", "", 'T', 0.1234567, Unit.ONE, 1);
    assertEquals("sample#T/0.123/1/", w.getBuffer().toString());
  }

  @Test
  public void writeLevel() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeLevel(w, "sample", 1.0, SI.SECOND, 2);
    assertEquals("sample#L/1000/2/ms", w.getBuffer().toString());
  }

  @Test
  public void writeTime() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeTime(w, "timer", 3.0, SI.MICRO(SI.SECOND), 3);
    assertEquals("timer#T/0.003/3/ms", w.getBuffer().toString());
  }

  @Test
  public void writeTimeInvalidUnit() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeTime(w, "timer", 3.0, Unit.ONE, 3);
    assertEquals("", w.getBuffer().toString());
  }

  @Test
  public void writeCount() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeCount(w, "counter", 4.0, Unit.ONE, 4);
    assertEquals("counter#C/4/4/", w.getBuffer().toString());
  }

  @Test
  public void beginGroup() throws Throwable {
    StringWriter w = new StringWriter();
    Group g = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","servicename").setAttribute("Operation","methodname").newGroup();
    format.writeBeginGroup(w, g);
    assertEquals("servicename:methodname=", w.getBuffer().toString());
  }

  @Test
  public void beginGroupServiceOnly() throws Throwable {
    StringWriter w = new StringWriter();
    Group g = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","servicename").newGroup();
    format.writeBeginGroup(w, g);
    assertEquals("servicename=", w.getBuffer().toString());
  }

  @Test
  public void endGroup() throws Throwable {
    StringWriter w = new StringWriter();
    format.writeEndGroup(w);
    assertEquals(";", w.getBuffer().toString());
  }

  @Test
  public void beginLine() throws Throwable {
    StringWriter w = new StringWriter();
    Group g = new GroupBuilder("ServiceMetrics").newGroup();
    format.writeBeginLine(w,g);
    assertEquals("ServiceMetrics=", w.getBuffer().toString());
  }

  @Test
  public void integrationTest() throws Throwable {
    // example from ServiceMetrics wiki page at
    // https://dse.amazon.com/?BSF:Monitoring:Service_Metrics#Parts_of_a_BSF_Service_Metric
    StringWriter w = new StringWriter();
    Group g = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","PARISService").setAttribute("Operation","getMarketplaceByID").newGroup();

    format.writeBeginLine(w,g);
    format.writeBeginGroup(w, g);
    format.writeCount(w, "BatchSize", 0, Unit.ONE, 7);
    format.writeSampleSeparator(w);
    format.writeLevel(w, "Latency", 0.123, SI.MILLI(SI.SECOND), 7);
    format.writeEndGroup(w);

    assertEquals("ServiceMetrics=PARISService:getMarketplaceByID=BatchSize#C/0/7/,Latency#L/0.123/7/ms;", w.getBuffer().toString());
  }

  @Test
  public void nonStandardAttributes() throws Throwable {
    // group attributes other than ServiceName, Operation are appended to the list after the standard ones, in alpha order of keys
    StringWriter w = new StringWriter();
    Group g = new GroupBuilder("ServiceMetrics").setAttribute("attr1","attr1value").setAttribute("ServiceName","service").setAttribute("otherAttrib","OtherValue").setAttribute("Operation","opname").setAttribute("1","2").newGroup();

    format.writeBeginGroup(w, g);
    format.writeEndGroup(w);

    assertEquals("service:opname:2:attr1value:OtherValue=;", w.getBuffer().toString());
  }

  @Test
  public void nonStandardAttributesNoOperation() throws Throwable {
    // check that these work without the presence of operation
    StringWriter w = new StringWriter();
    Group g = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","service").setAttribute("attr1","attr1value").setAttribute("otherAttrib","OtherValue").setAttribute("1","2").newGroup();

    format.writeBeginGroup(w, g);
    format.writeEndGroup(w);

    assertEquals("service:2:attr1value:OtherValue=;", w.getBuffer().toString());
  }

}
