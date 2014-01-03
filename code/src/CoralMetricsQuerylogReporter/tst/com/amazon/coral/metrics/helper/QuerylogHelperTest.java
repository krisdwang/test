package com.amazon.coral.metrics.helper;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.coral.metrics.*;

public class QuerylogHelperTest {

  @Test
  public void defaults() {
    QuerylogHelper h = new QuerylogHelper() {
      @Override
      protected void startJmxHelper() {}
    };
    assertTrue(h.getMetricsFactory() instanceof NullMetricsFactory);
  }

  @Test
  public void disabled() {
    QuerylogHelper h = new QuerylogHelper() {
      @Override
      protected void startJmxHelper() {}
    };
    h.setFilename("/tmp/querylog_helper");
    h.setEnabled(false);

    assertTrue(h.getMetricsFactory() instanceof NullMetricsFactory);
  }

  @Test
  public void querylog() {
    QuerylogHelper h = new QuerylogHelper() {
      @Override
      protected void startJmxHelper() {}
    };
    h.setFilename("/tmp/querylog_helper");

    assertTrue(h.getMetricsFactory() instanceof DefaultMetricsFactory);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void caching() {
    QuerylogHelper h = new QuerylogHelper() {
      @Override
      protected void startJmxHelper() {}
    };
    MetricsFactory m1 = h.newMetrics().getMetricsFactory(); // getMetricsFactory() is deprecated
    MetricsFactory m2 = h.newMetrics().getMetricsFactory();

    assertEquals(m1,m2);
  }

  @Test
  public void sampling() {
    QuerylogHelper h = new QuerylogHelper() {
      @Override
      protected void startJmxHelper() {}
    };
    h.setFilename("/tmp/querylog_helper");
    h.setSamplingRate(1.0);

    String reporterClassName = h.newReporter().getClass().getSimpleName();
    assertEquals("SamplingReporter", reporterClassName);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void metricsFactory() {
    QuerylogHelper h = new QuerylogHelper() {
      @Override
      protected void startJmxHelper() {}
    };

    assertSame(h, h.newMetrics().getMetricsFactory()); // getMetricsFactory() is deprecated
  }
}
