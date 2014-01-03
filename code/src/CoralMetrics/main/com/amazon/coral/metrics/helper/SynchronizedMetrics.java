package com.amazon.coral.metrics.helper;

import javax.measure.unit.Unit;

import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;

/**
 * A {@link Metrics} object that is thread-safe via synchronization on all of its
 * methods.
 * <p>
 * Synchronization concerns: If necessary to perform multiple operations atomically, the
 * caller may explicitly synchronize on an instance of this class.
 * </p>
 */
class SynchronizedMetrics extends DelegateMetrics {

    protected SynchronizedMetrics(final Metrics delegate) {
        super(delegate);
    }

    @Override
    public synchronized void addCount(final String name, final double value, final Unit<?> unit, final int repeat) {
        super.addCount(name, value, unit, repeat);
    }

    @Override
    public synchronized void addDate(final String name, final double value) {
        super.addDate(name, value);
    }

    @Override
    public synchronized void addLevel(final String name, final double value, final Unit<?> unit, final int repeat) {
        super.addLevel(name, value, unit, repeat);
    }

    @Override
    public synchronized void addProperty(final String name, final CharSequence value) {
        super.addProperty(name, value);
    }

    @Override
    public synchronized void addTime(final String name, final double value, final Unit<?> unit, final int repeat) {
        super.addTime(name, value, unit, repeat);
    }

    @Override
    public synchronized void close() {
        super.close();
    }

    @Override
    @Deprecated
    public synchronized MetricsFactory getMetricsFactory() {
        return super.getMetricsFactory(); // also deprecated
    }

    @Override
    public synchronized Metrics newMetrics(final Group group) {
        return super.newMetrics(group);
    }
}
