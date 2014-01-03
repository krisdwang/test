package com.amazon.coral.metrics;

/**
 * A <tt>MetricsFactory</tt> allows for the creation of unrelated
 * <b>MetricsRecords</b>. To begin collecting metrics, you'd instantitate
 * a concrete instance of a <tt>MetricsFactory</tt> and pass it into
 * your application.
 *
 * <code>
 * // The concrete instance you select might emit a querylog, or it might
 * // enable some MBeans that collect samples.
 * MetricsFactory factory = new NullMetricsFactory();
 * Metrics metrics = factory.newMetrics();
 * metrics.addCount("Example", 1.0);
 * metrics.close();
 * </code>
 *
 * @author Eric Crahen <crahen@amazon.com>
 */
public interface MetricsFactory {

  /**
   * Create a new <tt>Metrics</tt> instance that can be used to contribute
   * <tt>Metrics Primitives</tt> to a new <b>MetricsRecords</b>.
   *
   * @return Metrics instance
   */
  public Metrics newMetrics();

}
