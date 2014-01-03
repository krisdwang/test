package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.*;

import org.junit.Test;
import java.io.StringWriter;
import static org.junit.Assert.*;
import javax.measure.unit.*;

public class ServiceMetricsBuilderTest {

  @Test
  public void singleEntry() throws Throwable {
    StringWriter w = new StringWriter();
    ServiceMetricsBuilder smb = new ServiceMetricsBuilder();
    Group g = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","svc").newGroup();
    smb.addCount(g,"a",1.0,Unit.ONE,1);
    smb.appendTo(w);

    assertEquals("ServiceMetrics=svc=a#C/1/1/;\n", w.toString());
  }

  @Test
  public void sameServiceName() throws Throwable {
    StringWriter w = new StringWriter();
    ServiceMetricsBuilder smb = new ServiceMetricsBuilder();
    Group g = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","svc").newGroup();
    smb.addCount(g,"a",1.0,Unit.ONE,1);
    smb.addTime(g,"b",1.0,SI.MILLI(SI.SECOND),1);
    smb.appendTo(w);

    assertEquals("ServiceMetrics=svc=a#C/1/1/,b#T/1/1/ms;\n", w.toString());
  }


  @Test
  public void differentServiceName() throws Throwable {
    StringWriter w = new StringWriter();
    ServiceMetricsBuilder smb = new ServiceMetricsBuilder();
    Group g1 = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","svc").newGroup();
    Group g2 = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","svc2").newGroup();

    smb.addCount(g1,"a",1.0,Unit.ONE,1);
    smb.addTime(g2,"b",1.0,SI.MILLI(SI.SECOND),1);
    smb.appendTo(w);

    assertEquals("ServiceMetrics=svc=a#C/1/1/;svc2=b#T/1/1/ms;\n", w.toString());
  }

  @Test
  public void differentGroupName() throws Throwable {
    StringWriter w = new StringWriter();
    ServiceMetricsBuilder smb = new ServiceMetricsBuilder();
    Group g1 = new GroupBuilder("ServiceMetrics").setAttribute("ServiceName","svc").newGroup();
    Group g2 = new GroupBuilder("Verbose").newGroup();

    smb.addCount(g1,"a",1.0,Unit.ONE,1);
    smb.addTime(g2,"b",1.0,SI.MILLI(SI.SECOND),1);
    smb.appendTo(w);

    assertEquals("ServiceMetrics=svc=a#C/1/1/;\nVerbose=b#T/1/1/ms;\n", w.toString());
  }

}
