package com.amazon.coral.availability;

import javax.measure.unit.Unit;
import com.amazon.coral.metrics.BrokenMetrics;

public class TrackedMetrics extends BrokenMetrics {
  private int shedCount = 0;
  private int throttleCount = 0;
  private String shedKey = null;
  private String throttledKey = null;

  public int getShedCount() {
    return this.shedCount;
  }

  public int getThrottleCount() {
    return this.throttleCount;
  }

  public String getShedKey() {
    return this.shedKey;
  }

  public String getThrottledKey() {
    return this.throttledKey;
  }

  @Override
  public void addCount(String name, double value, Unit<?> unit, int repeat) {
    if ("LoadShed".equals(name))
      this.shedCount += (int)(value * repeat);
    else if ("Throttle".equals(name))
      this.throttleCount += (int)(value * repeat);
  }

  @Override
  public void addTime(String name, double value, Unit<?> unit, int repeat) { }

  @Override
  public void addProperty(String name, CharSequence value) {
    if ("ShedKey".equals(name))
      this.shedKey = value.toString();
    else if ("ThrottledKey".equals(name))
      this.throttledKey = value.toString();
  }
}
