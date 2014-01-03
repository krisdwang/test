package com.amazon.coral.metrics.helper;

import javax.measure.unit.Unit;

import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;

/**
 * This class is a sentinel metrics implementation which is activated when a
 * ClosureTolerantCompatibilityMetrics container is closed. The implementation
 * defines the compatibility of a metrics object that has entered into a
 * 'closure tolerant' state.
 * 
 * @author yamanoha
 * 
 */
class ClosureTolerantCompatibilityMetrics extends Metrics {

  /**
   * The metrics object whose post closure behaviour is undesirable.
   */
  private Metrics wrappedMetrics;

  /**
   * Constructs a new closure tolerant compatibility metrics.
   * 
   * @param wrappedMetrics
   *          The metrics object whose post closure behaviour is undesirable.
   */
  public ClosureTolerantCompatibilityMetrics(Metrics wrappedMetrics) {
    this.wrappedMetrics = wrappedMetrics;
  }

  @SuppressWarnings("deprecation")
  @Override
  public MetricsFactory getMetricsFactory() {
    // Compatibility: DELEGATE
    // Reason: Concrete metrics factory type can not be assumed.
    return this.wrappedMetrics.getMetricsFactory();
  }

  @Override
  public Metrics newMetrics(Group group) {
    // Compatibility: DELEGATE
    // Reason: Concrete metrics type can not be assumed.
    return this.wrappedMetrics.newMetrics(group);
  }

  @Override
  public void close() {
    // Compatibility: NOOP
    // Reason: To avoid any potential exceptions from post-close invocations.
    // https://issues.amazon.com/issues/e96c4538-69c6-4f44-bac0-39782f555848
  }

  @Override
  public void addProperty(String name, CharSequence value) {
    // Compatibility: NOOP
    // Reason: To avoid any potential exceptions from post-close invocations.
    // https://issues.amazon.com/issues/e96c4538-69c6-4f44-bac0-39782f555848
  }

  @Override
  public void addDate(String name, double value) {
    // Compatibility: NOOP
    // Reason: To avoid any potential exceptions from post-close invocations.
    // https://issues.amazon.com/issues/e96c4538-69c6-4f44-bac0-39782f555848
  }

  @Override
  public void addCount(String name, double value, Unit<?> unit, int repeat) {
    // Compatibility: NOOP
    // Reason: To avoid any potential exceptions from post-close invocations.
    // https://issues.amazon.com/issues/e96c4538-69c6-4f44-bac0-39782f555848
  }

  @Override
  public void addLevel(String name, double value, Unit<?> unit, int repeat) {
    // Compatibility: NOOP
    // Reason: To avoid any potential exceptions from post-close invocations.
    // https://issues.amazon.com/issues/e96c4538-69c6-4f44-bac0-39782f555848
  }

  @Override
  public void addTime(String name, double value, Unit<?> unit, int repeat) {
    // Compatibility: NOOP
    // Reason: To avoid any potential exceptions from post-close invocations.
    // https://issues.amazon.com/issues/e96c4538-69c6-4f44-bac0-39782f555848
  }
}
