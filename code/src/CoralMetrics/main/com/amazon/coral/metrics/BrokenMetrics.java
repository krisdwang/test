package com.amazon.coral.metrics;

import javax.measure.unit.Unit;
import javax.measure.unit.SI;

/**
 * A {@code BrokenMetrics} instance is a BrokenObject thats a 
 * useful tool in testing. This simplifies the task of writing
 * unit tests that ensure that only the metrics you expect to
 * be emited are actually emitted.
 *
 * @author Eric Crahen <crahen@amazon.com>
 * @version 1.0
 */
public class BrokenMetrics extends Metrics {

  @Override
  public MetricsFactory getMetricsFactory() {
    throw new UnsupportedOperationException();
  }

  /**
   */
  @Override
  public Metrics newMetrics(Group group) {
    throw new UnsupportedOperationException();
  }

  /**
   */
  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  /**
   */
  @Override
  public void addProperty(String name, CharSequence value) {
    throw new UnsupportedOperationException();
  }

  /**
   */
  @Override
  public void addDate(String name, double value) {
    throw new UnsupportedOperationException();
  }

  /**
   */
  @Override
  public void addCount(String name, double value, Unit<?> unit, int repeat) {
    throw new UnsupportedOperationException();
  }

  /**
   */
  @Override
  public void addLevel(String name, double value, Unit<?> unit, int repeat) {
    throw new UnsupportedOperationException();
  }

  /**
   */
  @Override
  public void addTime(String name, double value, Unit<?> unit, int repeat) {
    throw new UnsupportedOperationException();
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
