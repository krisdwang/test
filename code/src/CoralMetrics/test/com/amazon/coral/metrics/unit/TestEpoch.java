package com.amazon.coral.metrics.unit;

import org.junit.Test;
import static org.junit.Assert.*;
import javax.measure.unit.Unit;
import javax.measure.unit.SI;

public class TestEpoch {

  @Test
  public void epochToString() {
    assertEquals("epoch", Epoch.UNIT.toString());
  }

}
