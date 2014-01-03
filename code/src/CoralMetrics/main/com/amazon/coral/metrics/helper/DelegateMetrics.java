package com.amazon.coral.metrics.helper;

import javax.measure.unit.Unit;

import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;

class DelegateMetrics extends Metrics {

    private volatile Metrics delegate; 

    DelegateMetrics(Metrics delegate) { 
      if(delegate == null)
        throw new IllegalArgumentException();

      this.delegate = delegate;
    }

    @Override
    public void addCount(String name, double value, Unit<?> unit, int repeat) {
      delegate.addCount(name, value, unit, repeat);
    }

    @Override
    public void addDate(String name, double value) {
      delegate.addDate(name, value);
    }

    @Override
    public void addLevel(String name, double value, Unit<?> unit, int repeat) {
      delegate.addLevel(name, value, unit, repeat);
    }

    @Override
    public void addProperty(String name, CharSequence value) {
      delegate.addProperty(name, value);
    }

    @Override
    public void addTime(String name, double value, Unit<?> unit, int repeat) {
      delegate.addTime(name, value, unit, repeat);
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public MetricsFactory getMetricsFactory() {
      return delegate.getMetricsFactory(); // also deprecated
    }

    @Override
    public Metrics newMetrics(Group group) {
      return delegate.newMetrics(group);
    }
    
    /**
     * Retrieves the delegated metrics instance.
     * @return The delegated metrics.
     */
    protected Metrics getMetrics() {
      return this.delegate;
    }
    
    /**
     * Sets the delegated metrics instance.
     * @param metrics The new delegated metrics instance.
     */
    protected void setMetrics(Metrics metrics){
      this.delegate = metrics;
    }
}
