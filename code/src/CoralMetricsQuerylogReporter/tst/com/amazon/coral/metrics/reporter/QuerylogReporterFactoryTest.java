package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.*;
import java.util.TimeZone;

public class QuerylogReporterFactoryTest {

  @Test(expected=IllegalArgumentException.class)
  public void garbageParameter() throws Throwable {
    new QuerylogReporterFactory(null, TimeZone.getTimeZone("UTC"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void garbageParameter2() throws Throwable {
    new QuerylogReporterFactory(new StringWriter(), null);
  }

  @Test(expected=IllegalArgumentException.class)
  public void garbageParameter3() throws Throwable {
    OutputStream out = null;
    new QuerylogReporterFactory(out);
  }

  @Test
  public void outputStreamOK() throws Throwable {
    OutputStream out = new ByteArrayOutputStream();
    assertNotNull(new QuerylogReporterFactory(out).newReporter());
  }

  @Test
  public void concurrency() throws Throwable {
    StringWriter writer = new StringWriter();
    final QuerylogReporterFactory factory = new QuerylogReporterFactory(writer, TimeZone.getTimeZone("UTC"), false, false);

    int n = 10;
    final int m = 100;

    Thread[] threads = new Thread[n];
    for(int i = 0; i < n; i++) {
      threads[i] = new Thread() {
        public void run() {
          for(int j = 0; j < m; j++) {
            Reporter r = factory.newReporter();
            r.beginReport();
            r.endReport();
          }
        }
      };
    }

    for(int i = 0; i < n; i++)
      threads[i].start();

    for(int i = 0; i < n; i++)
      threads[i].join();

    StringBuilder expected = new StringBuilder();
    for(int i = 0; i < n * m; i++)
      expected.append("------------------------------------------------------------------------\n" +
        "Operation=Unknown\n" +
        "EOE\n");

    assertEquals(expected.toString(), writer.toString());
  }

}
