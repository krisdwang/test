package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.*;

public class QuerylogDOMTest {
  @Test
  public void loadQuerylogLine() {
    QuerylogDOM.QuerylogLine l = new QuerylogDOM.QuerylogLine("name=value");
    assertEquals("name", l.getName());
    assertEquals("value", l.getRawValue());
  }
  @Test
  public void printQuerylogLine() {
    QuerylogDOM.QuerylogLine l = new QuerylogDOM.QuerylogLine("name","value");
    assertEquals("name=value\n", l.toString());
  }
  @Test
  public void equalsQuerylogLine() {
    QuerylogDOM.QuerylogLine l1 = new QuerylogDOM.QuerylogLine("name=value");
    QuerylogDOM.QuerylogLine l2 = new QuerylogDOM.QuerylogLine("name","value");
    assertEquals(l1, l2);
    assertEquals(l2, l1);
  }
  @Test
  public void notEqualsQuerylogLine() {
    QuerylogDOM.QuerylogLine l1 = new QuerylogDOM.QuerylogLine("name=value");
    QuerylogDOM.QuerylogLine l2 = new QuerylogDOM.QuerylogLine("name=value2");
    assertFalse(l1.equals(l2));
    assertFalse(l2.equals(l1));
  }

  @Test
  public void loadServiceMetricsMetric() {
    QuerylogDOM.ServiceMetricsMetric m = new QuerylogDOM.ServiceMetricsMetric("metric:critical#T/1/1/ms");
    assertEquals("metric",m.getName());
    assertEquals("critical",m.getCriticality());
    assertEquals("T",m.getType());
    assertEquals("1",m.getValue());
    assertEquals("1",m.getRepeat());
    assertEquals("ms",m.getUnits());

    m = new QuerylogDOM.ServiceMetricsMetric("metric:critical#T/1/1/");
    assertEquals("metric",m.getName());
    assertEquals("critical",m.getCriticality());
    assertEquals("T",m.getType());
    assertEquals("1",m.getValue());
    assertEquals("1",m.getRepeat());
    assertEquals("",m.getUnits());

    m = new QuerylogDOM.ServiceMetricsMetric("metric#T/1/1/ms");
    assertEquals("metric",m.getName());
    assertEquals("",m.getCriticality());
    assertEquals("T",m.getType());
    assertEquals("1",m.getValue());
    assertEquals("1",m.getRepeat());
    assertEquals("ms",m.getUnits());
  }
  @Test
  public void printServiceMetricsMetric() {
    QuerylogDOM.ServiceMetricsMetric m = new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms");
    assertEquals("metric:critical#T/1/1/ms", m.toString());
  }
  @Test
  public void equalsServiceMetricsMetric() {
    QuerylogDOM.ServiceMetricsMetric m1 = new QuerylogDOM.ServiceMetricsMetric("metric:critical#T/1/1/ms");
    QuerylogDOM.ServiceMetricsMetric m2 = new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms");
    assertEquals(m1,m2);
    assertEquals(m2,m1);
  }
  @Test
  public void notEqualsServiceMetricsMetric() {
    QuerylogDOM.ServiceMetricsMetric m1 = new QuerylogDOM.ServiceMetricsMetric("metric:critical#T/1/1/ms");
    QuerylogDOM.ServiceMetricsMetric m2 = new QuerylogDOM.ServiceMetricsMetric("metric:critical#T/2/1/ms");
    assertFalse(m1.equals(m2));
    assertFalse(m2.equals(m1));
  }

  @Test
  public void equalsServiceMetricsGroup() {
    QuerylogDOM.ServiceMetricsGroup g1 = new QuerylogDOM.ServiceMetricsGroup();
    g1.addAttribute("service");
    g1.addAttribute("operation");
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms"));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric2","","C","1","7",""));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric3","","L","4","1","%"));

    String str = "service:operation=metric:critical#T/1/1/ms,metric2#C/1/7/,metric3#L/4/1/%";
    QuerylogDOM.ServiceMetricsGroup g2 = new QuerylogDOM.ServiceMetricsGroup(str);

    // can't do string compare due to reordering
    //assertEquals(str, g1.toString());

    assertEquals(g1,g2);
    assertEquals(g2,g1);
  }
  @Test
  public void notEqualsServiceMetricsGroup() {
    QuerylogDOM.ServiceMetricsGroup g1 = new QuerylogDOM.ServiceMetricsGroup();
    g1.addAttribute("service");
    g1.addAttribute("operation");
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms"));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric2","","C","99","7",""));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric3","","L","4","1","%"));

    String equivalent = "service:operation=metric:critical#T/1/1/ms,metric2#C/1/7/,metric3#L/4/1/%";
    QuerylogDOM.ServiceMetricsGroup g2 = new QuerylogDOM.ServiceMetricsGroup(equivalent);

    assertFalse(g1.equals(g2));
    assertFalse(g2.equals(g1));
  }

  @Test
  public void equalsServiceMetricsLine() {
    QuerylogDOM.ServiceMetricsGroup g1 = new QuerylogDOM.ServiceMetricsGroup();
    g1.addAttribute("service");
    g1.addAttribute("operation");
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms"));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric2","","C","1","7",""));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric3","","L","4","1","%"));


    QuerylogDOM.ServiceMetricsGroup g2 = new QuerylogDOM.ServiceMetricsGroup();
    g2.addAttribute("service2");
    g2.addAttribute("operation2");
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric4","","T","1","1","ms"));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric5","critical","C","1","7",""));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric6","","L","4","1","%"));


    QuerylogDOM.ServiceMetricsLine l1 = new QuerylogDOM.ServiceMetricsLine("ServiceMetrics");
    l1.addGroup(g1).addGroup(g2);

    String equivalent = "ServiceMetrics=service:operation=metric:critical#T/1/1/ms,metric2#C/1/7/,metric3#L/4/1/%;service2:operation2=metric4#T/1/1/ms,metric5:critical#C/1/7/,metric6#L/4/1/%;";

    QuerylogDOM.ServiceMetricsLine l2 = new QuerylogDOM.ServiceMetricsLine(0,equivalent);

    assertEquals(l1,l2);
    assertEquals(l2,l1);
  }

  @Test
  public void notEqualsServiceMetricsLine() {
    QuerylogDOM.ServiceMetricsGroup g1 = new QuerylogDOM.ServiceMetricsGroup();
    g1.addAttribute("service");
    g1.addAttribute("operation");
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms"));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric2","","C","1","7",""));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric3","","L","4","1","%"));

    QuerylogDOM.ServiceMetricsGroup g2 = new QuerylogDOM.ServiceMetricsGroup();
    g2.addAttribute("service2");
    g2.addAttribute("operation2");
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric4","","T","1","1","ms"));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric5","critical","C","1","799",""));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric6","","L","4","1","%"));


    QuerylogDOM.ServiceMetricsLine l1 = new QuerylogDOM.ServiceMetricsLine("ServiceMetrics");
    l1.addGroup(g1).addGroup(g2);

    String notEquivalent = "ServiceMetrics=service:operation=metric:critical#T/1/1/ms,metric2#C/1/7/,metric3#L/4/1/%;service2:operation2=metric4#T/1/1/ms,metric5:critical#C/1/7/,metric6#L/4/1/%;";

    QuerylogDOM.ServiceMetricsLine l2 = new QuerylogDOM.ServiceMetricsLine(0,notEquivalent);

    assertFalse(l1.equals(l2));
    assertFalse(l2.equals(l1));
  }

  @Test
  public void equalsQuerylogRecord() throws Throwable {

    QuerylogDOM.ServiceMetricsGroup g1 = new QuerylogDOM.ServiceMetricsGroup();
    g1.addAttribute("service");
    g1.addAttribute("operation");
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms"));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric2","","C","1","7",""));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric3","","L","4","1","%"));

    QuerylogDOM.ServiceMetricsGroup g2 = new QuerylogDOM.ServiceMetricsGroup();
    g2.addAttribute("service2");
    g2.addAttribute("operation2");
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric4","","T","1","1","ms"));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric5","critical","C","1","7",""));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric6","","L","4","1","%"));


    QuerylogDOM.ServiceMetricsLine l1 = new QuerylogDOM.ServiceMetricsLine("ServiceMetrics");
    l1.addGroup(g1).addGroup(g2);

    QuerylogDOM.QuerylogRecord r1 = new QuerylogDOM.QuerylogRecord();
    r1.addLine(l1);
    r1.addLine(new QuerylogDOM.QuerylogLine("property","value"));
    r1.addLine(new QuerylogDOM.QuerylogLine("EndTime","Mon, 25 Feb 2002 23:27:34 GMT"));

    String equivalent = "ServiceMetrics=service:operation=metric:critical#T/1/1/ms,metric2#C/1/7/,metric3#L/4/1/%;service2:operation2=metric4#T/1/1/ms,metric5:critical#C/1/7/,metric6#L/4/1/%;\nproperty=value\nEndTime=Mon, 25 Feb 2002 23:27:34 GMT\nEOE";

    QuerylogDOM.QuerylogRecord r2 = new QuerylogDOM.QuerylogRecord(new BufferedReader(new StringReader(equivalent)));

    assertEquals(r1,r2);
    assertEquals(r2,r1);
  }
  @Test
  public void notEqualsQuerylogRecord() throws Throwable {

    QuerylogDOM.ServiceMetricsGroup g1 = new QuerylogDOM.ServiceMetricsGroup();
    g1.addAttribute("service");
    g1.addAttribute("operation");
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms"));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric2","","C","1","7",""));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric3","","L","4","1","%"));

    QuerylogDOM.ServiceMetricsGroup g2 = new QuerylogDOM.ServiceMetricsGroup();
    g2.addAttribute("service2");
    g2.addAttribute("operation2");
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric4","","T","1","1","ms"));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric5","critical","C","1","7",""));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metricFoo","","L","4","1","%"));


    QuerylogDOM.ServiceMetricsLine l1 = new QuerylogDOM.ServiceMetricsLine("ServiceMetrics");
    l1.addGroup(g1).addGroup(g2);

    QuerylogDOM.QuerylogRecord r1 = new QuerylogDOM.QuerylogRecord();
    r1.addLine(l1);
    r1.addLine(new QuerylogDOM.QuerylogLine("property","value"));
    r1.addLine(new QuerylogDOM.QuerylogLine("EndTime","Mon, 25 Feb 2002 23:27:34 GMT"));

    String equivalent = "ServiceMetrics=service:operation=metric:critical#T/1/1/ms,metric2#C/1/7/,metric3#L/4/1/%;service2:operation2=metric4#T/1/1/ms,metric5:critical#C/1/7/,metric6#L/4/1/%;\nproperty=value\nEndTime=Mon, 25 Feb 2002 23:27:34 GMT\nEOE";

    QuerylogDOM.QuerylogRecord r2 = new QuerylogDOM.QuerylogRecord(new BufferedReader(new StringReader(equivalent)));

    assertFalse(r1.equals(r2));
    assertFalse(r2.equals(r1));
  }

  @Test
  public void equalsQuerylogDOM() throws Throwable {

    QuerylogDOM.ServiceMetricsGroup g1 = new QuerylogDOM.ServiceMetricsGroup();
    g1.addAttribute("service");
    g1.addAttribute("operation");
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms"));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric2","","C","1","7",""));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric3","","L","4","1","%"));

    QuerylogDOM.ServiceMetricsGroup g2 = new QuerylogDOM.ServiceMetricsGroup();
    g2.addAttribute("service2");
    g2.addAttribute("operation2");
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric4","","T","1","1","ms"));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric5","critical","C","1","7",""));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric6","","L","4","1","%"));


    QuerylogDOM.ServiceMetricsLine l1 = new QuerylogDOM.ServiceMetricsLine("ServiceMetrics");
    l1.addGroup(g1).addGroup(g2);

    QuerylogDOM.QuerylogRecord r1 = new QuerylogDOM.QuerylogRecord();
    r1.addLine(l1);
    r1.addLine(new QuerylogDOM.QuerylogLine("property","value"));
    r1.addLine(new QuerylogDOM.QuerylogLine("EndTime","Mon, 25 Feb 2002 23:27:34 GMT"));

    QuerylogDOM.QuerylogRecord r2 = new QuerylogDOM.QuerylogRecord().addLine(new QuerylogDOM.QuerylogLine("property2","value2")).addLine(new QuerylogDOM.ServiceMetricsLine("Verbose").addGroup(new QuerylogDOM.ServiceMetricsGroup().addMetric(new QuerylogDOM.ServiceMetricsMetric("metric7","","L","41","13","%"))));

    QuerylogDOM d1 = new QuerylogDOM();
    d1.addRecord(r1).addRecord(r2);

    String equivalent = "" +
"------------------------------------------------------------------------\n" +
"ServiceMetrics=service:operation=metric:critical#T/1/1/ms,metric2#C/1/7/,metric3#L/4/1/%;service2:operation2=metric4#T/1/1/ms,metric5:critical#C/1/7/,metric6#L/4/1/%;\n" +
"property=value\n" +
"EndTime=Mon, 25 Feb 2002 23:27:34 GMT\n" +
"EOE\n" +
"------------------------------------------------------------------------\n" +
"property2=value2\n" +
"Verbose=metric7#L/41/13/%;\n" +
"EOE\n";

    QuerylogDOM d2 = new QuerylogDOM(new BufferedReader(new StringReader(equivalent)));

    assertEquals(d1,d2);
    assertEquals(d2,d1);

  }

  @Test
  public void notEqualsQuerylogDOM() throws Throwable {

    QuerylogDOM.ServiceMetricsGroup g1 = new QuerylogDOM.ServiceMetricsGroup();
    g1.addAttribute("service");
    g1.addAttribute("operation");
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric","critical","T","1","1","ms"));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric2","","C","1","7",""));
    g1.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric3","","L","4","1","%"));

    QuerylogDOM.ServiceMetricsGroup g2 = new QuerylogDOM.ServiceMetricsGroup();
    g2.addAttribute("service2");
    g2.addAttribute("operation2");
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric4","","T","1","1","ms"));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric5","critical","C","1","7",""));
    g2.addMetric(new QuerylogDOM.ServiceMetricsMetric("metric6","","L","4","1","%"));


    QuerylogDOM.ServiceMetricsLine l1 = new QuerylogDOM.ServiceMetricsLine("ServiceMetrics");
    l1.addGroup(g1).addGroup(g2);

    QuerylogDOM.QuerylogRecord r1 = new QuerylogDOM.QuerylogRecord();
    r1.addLine(l1);
    r1.addLine(new QuerylogDOM.QuerylogLine("property","value"));
    r1.addLine(new QuerylogDOM.QuerylogLine("EndTime","Mon, 25 Feb 2002 23:27:34 GMT"));

    QuerylogDOM.QuerylogRecord r2 = new QuerylogDOM.QuerylogRecord().addLine(new QuerylogDOM.QuerylogLine("property2","value2")).addLine(new QuerylogDOM.ServiceMetricsLine("Verbose").addGroup(new QuerylogDOM.ServiceMetricsGroup().addMetric(new QuerylogDOM.ServiceMetricsMetric("metric7","","L","41","13","%"))));

    QuerylogDOM d1 = new QuerylogDOM();
    d1.addRecord(r1).addRecord(r2);

    String equivalent = "" +
"------------------------------------------------------------------------\n" +
"ServiceMetrics=service:operation=metric:critical#T/1/1/ms,metric2#C/1/7/,metric3#L/4/1/%;service2:operation2=metric4#T/1/1/ms,metric5:critical#C/1/7/,metric6#L/4/1/%;\n" +
"property=value\n" +
"EndTime=Mon, 25 Feb 2002 23:27:34 GMT\n" +
"EOE\n" +
"------------------------------------------------------------------------\n" +
"property2=value2\n" +
"Verbose=metric8#L/41/13/%;\n" +
"EOE\n";

    QuerylogDOM d2 = new QuerylogDOM(new BufferedReader(new StringReader(equivalent)));

    assertFalse(d1.equals(d2));
    assertFalse(d2.equals(d1));
  }

}
