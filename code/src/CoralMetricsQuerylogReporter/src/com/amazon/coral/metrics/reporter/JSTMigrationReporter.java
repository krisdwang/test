package com.amazon.coral.metrics.reporter;

import java.util.concurrent.atomic.AtomicLong;

import com.amazon.coral.metrics.Group;
import static com.amazon.coral.metrics.GroupBuilder.DEFAULT_GROUP;
import javax.measure.unit.Unit;

/**
 * Querylog reporter wrapper that translates metrics into legacy JST-style
 * metrics.  This wrapper is intended for services migrating off of JST that
 * don't want to tweak their dashboards, alarms, etc to use the operation
 * naming style that the Coral uses.  The JST style that this
 * wrapper provides looks like:
 *
 * Program=&lt;Configured Program Name&rt;
 * Operation=&lt;ServiceInterface&rt;.&lt;Method&rt;
 *
 *
 * The Coral-style looks like:
 *
 * Program=&lt;ServiceInterface&rt;
 * Operation=&lt;Method&rt;
 *
 * We recommend you don't use this wrapper and use the Coral-style instead, as
 * it will be more standard and performs more correct metric aggregation when
 * exposing multiple service interfaces from the same stack.  This wrapper is
 * intended only as a convenience mechanism if changing your existing JST
 * graphs/alarms is too much effort.
 *
 * @author Dave Killian <killian@amazon.com>
 */
class JSTMigrationReporter implements Reporter {


  private final Reporter delegate;
  private final AtomicLong count = new AtomicLong(0);

  private CharSequence service;
  private CharSequence operation;

  public JSTMigrationReporter(Reporter delegate) {
    if(delegate == null)
      throw new IllegalArgumentException();

    this.delegate = delegate;
  }

  /**
   */
  public void beginReport() {
    count.incrementAndGet();
    delegate.beginReport();
  }

  /**
   */
  public void addProperty(Group group, String name, CharSequence value) {

    if(DEFAULT_GROUP.equals(group)) {
      if("Operation".equals(name)) {
        operation = value;
        return;
      }

      if("Service".equals(name)) {
        // don't delegate service, it will become part of the operation name
        service = value;
        return;
      }
    }

    delegate.addProperty(group, name, value);
  }

  /**
   */
  public void addDate(Group group, String name, double value) {
    delegate.addDate(group, name, value);
  }

  /**
   */
  public void addCount(Group group, String name, double value, Unit<?> unit, int count) {
    delegate.addCount(group, name, value, unit, count);
  }

  /**
   */
  public void addTime(Group group, String name, double value, Unit<?> unit, int count) {
    delegate.addTime(group, name, value, unit, count);
  }

  /**
   */
  public void addLevel(Group group, String name, double value, Unit<?> unit, int count) {
    delegate.addLevel(group, name, value, unit, count);
  }

  /**
   */
  public void endReport() {

    // if we have a service interface, prepend it to the operation
    if(count.decrementAndGet() == 0) {
      if (service != null && operation != null)
        operation = service + "." + operation;

      if(operation != null)
        delegate.addProperty(DEFAULT_GROUP, "Operation", operation);
    }

    delegate.endReport();
  }
}
