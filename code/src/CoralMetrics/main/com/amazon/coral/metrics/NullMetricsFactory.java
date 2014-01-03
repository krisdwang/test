package com.amazon.coral.metrics;

/**
 */
public class NullMetricsFactory implements MetricsFactory {

  /**
   */
  public Metrics newMetrics() {
    return new NullMetrics(this);
  }

}

