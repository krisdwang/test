package com.amazon.coral.metrics;

import javax.measure.unit.Unit;
import javax.measure.unit.SI;

/**
 * A <tt>Metrics</tt> instance allows the expression of a closely related group of
 * <tt>Metrics Primatives</tt>.
 *
 * @author Eric Crahen <crahen@amazon.com>
 * @version 1.0
 */
public abstract class Metrics {

  /**
   * Deprecated Inject the MetricsFactory where necessary rather than use this.
   */
  public abstract MetricsFactory getMetricsFactory();

  /**
   * Create a new Metrics instance using the default group.
   * The Metrics object on which this method is invoked must not
   * already have been closed.
   */
  public Metrics newMetrics() {
    return newMetrics(GroupBuilder.DEFAULT_GROUP);
  }

  /**
   * Create a new Metrics instance that can be used to collect  
   * related information that is part of the same <b>Metrics Record</b>
   * as this object, but is aggregated differently.
   *
   * For example,
   *
   * <code>
   * void f(Metrics m) {
   *
   *   Group g = new Group("ServiceMetrics");
   *   Metrics m2 = m.newMetrics(g);
   *   m2.addProperty("ServiceName", "aService");
   *   m2.addCount("aCount", 1.0, Unit.ONE);
   *   m2.close();
   *
   * }
   * </code>
   *
   * The above function would contribute data to the ServiceMetrics group of 
   * <b>Metrics Record</b>. Furthermore,
   *
   * <code>
   * void g(Metrics m) {
   *   for(int i = 0; i < asManyTimesAsYouWant; i++)
   *     f(m);
   * }
   * </code>
   *
   * You can open and close new <em>instances</em> of the same group as many
   * times as is needed.  The Metrics object on which this method is invoked
   * must not already have been closed.
   *
   * @param group
   */
  public abstract Metrics newMetrics(Group group);

  /**
   * Close this Metrics object. No further <tt>Metrics Primitives</tt> may
   * be expressed once a Metrics object has been closed. Once closed,
   * a Metrics object can not be reopened.  Collaborating metrics objects
   * may be closed in any order; each metrics object must be closed at least
   * once.
   */
  public abstract void close();

  /**
   * Add a property to the group of <tt>Metrics Primatives</tt>. These
   * properties are often considered during the aggregation by
   * <tt>Reporters</tt>.
   *
   * For example,
   *
   * <code>
   * void f(Metrics m) {
   *   m.addProperty("ServiceName", "aService");
   *   m.addProperty("Operation", "anOperation");
   * }
   * </code>
   *
   * the above code would add two properties which would be quite
   * helpful to a <tt>Reporter</tt> formating Query log records.
   *
   * @param name attribute name
   * @param value attribute value
   */
  public abstract void addProperty(String name, CharSequence value);

  /**
   * Add a date to the group of <tt>Metrics Primatives</tt>. These
   * dates are often considered during the aggregation by
   * <tt>Reporters</tt>.
   *
   * For example,
   *
   * <code>
   * void f(Metrics m) {
   *   m.addDate("StartTime", System.currentTimeMillis() );
   *   m.addDate("EndTime", System.currentTimeMillis() + 1000.0 );
   * }
   * </code>
   *
   * the above code would add two dates which would be quite
   * helpful to a <tt>Reporter</tt> formating Query log records.
   *
   * @param name attribute name
   * @param value date value in milliseconds since epoch.
   *    To retrieve this value for the current time, see System.currentTimeMillis().
   */

  public abstract void addDate(String name, double value);

  /**
   * Increment a count primitive by <em>value</em>, setting it equal to <em>value</em> if 
   * the named count had not existed yet.
   * 
   * @param name count name
   * @param value count increment
   * @param unit the unit to affix
   *
   * @see #addCount(String, double, Unit<?>, int)
   */
  public void addCount(String name, double value, Unit<?> unit) {
    addCount(name, value, unit, 1);
  }

  /**
   * Increment a count primitive by <em>value</em>, setting it equal to <em>value</em> if 
   * the named count had not existed yet.  Calling this method may be more efficient 
   * than calling <tt>addCount</tt> repeatedly.
   * 
   * For example,
   *
   * <code>
   * void f(Metrics m) {
   *   m.addCount("ScreenRealEstate", 2.0, NonSI.PIXEL, 1000000 );
   * }
   * </code>
   *
   * The above code would accumulate a count named "ScreenRealEstate" of 2 pixels.
   *
   * @param name count name
   * @param value count increment
   * @param unit the unit to affix
   * @param repeat the number of samples from which <em>value</em> was accumulated
   *
   * @see #addCount(String, double, Unit<?>)
   */
  public abstract void addCount(String name, double value, Unit<?> unit, int repeat);

  /**
   * Update a level primitive by factoring <em>value</em> into the running
   * average associated with the named level.
   *
   * @param name level name
   * @param value level value
   * @param unit the unit to affix
   *
   * @see #addLevel(String, double, Unit<?>, int)
   */
  public void addLevel(String name, double value, Unit<?> unit) {
    addLevel(name, value, unit, 1);
  }

  /**
   * Update a level primitive by factoring <em>value</em> into the running
   * average associated with the named level. Calling this method may be 
   * more efficient than calling <tt>addLevel</tt> repeatedly.
   *
   * @param name level name
   * @param value level value
   * @param unit the unit to affix
   * @param repeat the number of samples from which <em>value</em> was accumulated
   * 
   * For example,
   *
   * <code>
   * void f(Metrics m, double travled) {
   *   m.addLevel("Velocity", traveled, NonSI.LIGHTYEAR.divide(NonSI.HOUR), 1 );
   * }
   * </code>
   *
   * The above code would update a level named "Velocity" keeping track of the average velocity of a spaceship.
   *
   * @see #addLevel(String, double, Unit<?>)
   */
  public abstract void addLevel(String name, double value, Unit<?> unit, int repeat);

  /**
   * Increment a time primitive by <em>value</em>, setting it equal to <em>value</em> if 
   * the named time had not existed yet.
   *
   * @param name count name
   * @param value count increment
   * @param unit the unit to affix
   *
   * @see #addTime(String, double, Unit<?>, int)
   */
  public void addTime(String name, double value, Unit<?> unit) {
    addTime(name, value, unit, 1);
  }

  /**
   * Increment a time primitive by <em>value</em>, setting it equal to <em>value</em> if 
   * the named time had not existed yet. Calling this method may be more efficient 
   * than calling <tt>addTime</tt> repeatedly.
   *
   * For example,
   *
   * <code>
   * void f(Metrics m) {
   *   m.addLevel("TimeSpentWritingCode", 1000, NonSI.HOUR, 1 );
   * }
   * </code>
   *
   * The above code would update a timer named "TimeSpentWritingCode" that was keeping track of 
   * how long you spent in front of a computer.
   *
   * @param name count name
   * @param value count increment
   * @param unit the unit to affix
   * @param repeat the number of samples from which <em>value</em> was accumulated
   *
   * @see #addTime(String, double, Unit<?>)
   */
  public abstract void addTime(String name, double value, Unit<?> unit, int repeat);

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
