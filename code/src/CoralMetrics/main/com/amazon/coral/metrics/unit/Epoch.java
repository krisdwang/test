package com.amazon.coral.metrics.unit;

import javax.measure.quantity.Quantity;
import javax.measure.unit.BaseUnit;
import javax.measure.unit.Unit;

/**
 * This interface represents a measure of time relative to
 * the epoch, 1/1/1970 UTC.
 */
public interface Epoch extends Quantity {
  public final static Unit<Epoch> UNIT = new BaseUnit<Epoch>("epoch");
}
