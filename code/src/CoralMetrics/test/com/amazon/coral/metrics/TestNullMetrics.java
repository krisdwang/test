package com.amazon.coral.metrics;

import org.junit.Test;
import static org.junit.Assert.*;
import javax.measure.unit.Unit;
import javax.measure.unit.SI;
import java.util.HashMap;

// This is a rough sanity check of the Null objet
// implementation. Basically, the expectations are
// that nothing bad should happen by doing 
// whatever you feel like to this object.
public class TestNullMetrics {

  private final NullMetricsFactory factory = new NullMetricsFactory();

  @Test
  public void inertClose() {
    new NullMetrics(factory).close();
  }

  @Test
  public void inertNewMetrics() {
    assertTrue(new NullMetrics(factory).newMetrics() instanceof NullMetrics);
  }

  @Test
  public void inertNewMetricsGroup() {
    Group g = new GroupBuilder("group").newGroup();
    assertTrue(new NullMetrics(factory).newMetrics(g) instanceof NullMetrics);
  }

  @Test
  public void inertAddCount() {
    new NullMetrics(factory).addCount(null, -1.0, null, -40);
  }

  @Test
  public void inertAddLevel() {
    new NullMetrics(factory).addLevel(null, -1.0, null, -40);
  }

  @Test
  public void inertAddTime() {
    new NullMetrics(factory).addTime(null, -1.0, null, -40);
  }

  @Test
  public void inertAddDate() {
    new NullMetrics(factory).addDate(null, -123);
  }

  @Test
  public void inertAddProperty() {
    new NullMetrics(factory).addProperty(null, null);
  }

  @Test
  public void inertFactory() {
    assertTrue(new NullMetricsFactory().newMetrics() instanceof NullMetrics);
  }

  @Test
  public void getFactory() {
    NullMetricsFactory nmf = new NullMetricsFactory();
    Metrics nm = nmf.newMetrics();

    assertEquals(nmf, nm.getMetricsFactory());
  }

}
