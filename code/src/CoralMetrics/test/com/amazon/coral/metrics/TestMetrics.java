package com.amazon.coral.metrics;

import org.junit.Test;
import static org.junit.Assert.*;
import javax.measure.unit.Unit;
import javax.measure.unit.SI;

public class TestMetrics {

  private final NullMetricsFactory factory = new NullMetricsFactory();

  @Test
  public void newMetricsDefaultIsDefaultGroup() {

    Group defaultGroup = new GroupBuilder().newGroup();

    final Group value[] = { null };
    new NullMetrics(factory) {
      @Override
      public Metrics newMetrics(Group group) {
        value[0] = group;
        return super.newMetrics(group);
      }
    }.newMetrics();
   
    assertEquals(defaultGroup, value[0]);

  }

  @Test
  public void addCountDefaultsRepeat() {

    String expectedName = "aName";
    double expectedValue = 123.4;
    Unit<?> expectedUnit = SI.BIT;

    final String name[] = { null };
    final double value[] = { 0.0 };
    final Unit unit[] = { null };
    final int repeat[] = { 0 };
    new NullMetrics(factory) {
      @Override
      public void addCount(String n, double v, Unit<?> u, int r) {
        name[0] = n;
        value[0] = v;
        unit[0] = u;
        repeat[0] = r;
      }
    }.addCount(expectedName, expectedValue, expectedUnit);

    assertEquals(expectedName, name[0]);
    assertEquals(expectedValue, value[0], 0);
    assertEquals(expectedUnit, unit[0]);
    assertEquals(1, repeat[0]);

  }

  @Test
  public void addLevelDefaultsRepeat() {

    String expectedName = "aName";
    double expectedValue = 123.4;
    Unit<?> expectedUnit = SI.BIT;

    final String name[] = { null };
    final double value[] = { 0.0 };
    final Unit unit[] = { null };
    final int repeat[] = { 0 };
    new NullMetrics(factory) {
      @Override
      public void addLevel(String n, double v, Unit<?> u, int r) {
        name[0] = n;
        value[0] = v;
        unit[0] = u;
        repeat[0] = r;
      }
    }.addLevel(expectedName, expectedValue, expectedUnit);

    assertEquals(expectedName, name[0]);
    assertEquals(expectedValue, value[0], 0);
    assertEquals(expectedUnit, unit[0]);
    assertEquals(1, repeat[0]);

  }

  @Test
  public void addTimeDefaultsRepeat() {

    String expectedName = "aName";
    double expectedValue = 123.4;
    Unit<?> expectedUnit = SI.BIT;

    final String name[] = { null };
    final double value[] = { 0.0 };
    final Unit unit[] = { null };
    final int repeat[] = { 0 };
    new NullMetrics(factory) {
      @Override
      public void addTime(String n, double v, Unit<?> u, int r) {
        name[0] = n;
        value[0] = v;
        unit[0] = u;
        repeat[0] = r;
      }
    }.addTime(expectedName, expectedValue, expectedUnit);

    assertEquals(expectedName, name[0]);
    assertEquals(expectedValue, value[0], 0);
    assertEquals(expectedUnit, unit[0]);
    assertEquals(1, repeat[0]);

  }

}
