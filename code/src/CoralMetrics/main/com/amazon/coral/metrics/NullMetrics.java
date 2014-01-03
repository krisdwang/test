package com.amazon.coral.metrics;

import javax.measure.unit.Unit;
import javax.measure.unit.SI;

/**
 * @author Eric Crahen <crahen@amazon.com>
 * @version 1.0
 */
class NullMetrics extends Metrics {

  private final NullMetricsFactory factory;

  NullMetrics(NullMetricsFactory factory) {
    this.factory = factory;
  }

  public NullMetricsFactory getMetricsFactory() {
    return factory;
  }

  /**
   */
  @Override
  public Metrics newMetrics(Group group) {
    return new NullMetrics(factory);
  }

  /**
   */
  @Override
  public void close() {
  }

  /**
   */
  @Override
  public void addProperty(String name, CharSequence value) {
  }

  /**
   */
  @Override
  public void addDate(String name, double value) {
  }

  /**
   */
  @Override
  public void addCount(String name, double value, Unit<?> unit, int repeat) {
  }

  /**
   */
  @Override
  public void addLevel(String name, double value, Unit<?> unit, int repeat) {
  }

  /**
   */
  @Override
  public void addTime(String name, double value, Unit<?> unit, int repeat) {
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
