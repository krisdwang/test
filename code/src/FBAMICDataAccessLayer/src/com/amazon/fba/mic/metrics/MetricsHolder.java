package com.amazon.fba.mic.metrics;

import java.util.LinkedList;

import javax.measure.unit.Unit;

import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.GroupBuilder;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;

/**
 * 
 * @author wdong
 * 
 */
public class MetricsHolder {

	private final EnsuredCloseMetrics primaryMetrics;
	private int refcount = 1;
	private LinkedList<EnsuredCloseMetrics> metricsList = null;
	private volatile boolean outOfScope = false;

	public MetricsHolder(Metrics metrics) {
		this.primaryMetrics = new EnsuredCloseMetrics(this, metrics);
	}

	private synchronized void registerMetrics(EnsuredCloseMetrics metrics) {
		if (metricsList == null) {
			metricsList = new LinkedList<EnsuredCloseMetrics>();
		}
		metricsList.add(metrics);
	}

	synchronized void ref() {
		++refcount;
	}

	void unref() {
		synchronized (this) {
			if (refcount == 0) {
				throw new IllegalStateException("refcount went negative on unref() call");
			} else if (refcount == 1) {
				outOfScope = true;
			}
			--refcount;
		}

		// close() is specifically outside of the synchronized block because
		// it may be slow (doing file output to the query log) and is already
		// appropriately synchronized

		if (outOfScope) {
			if (metricsList != null) {
				for (EnsuredCloseMetrics metrics : metricsList) {
					metrics.close();
				}
			}
			primaryMetrics.close();
		}
	}

	public Metrics newMetrics() {
		return newMetrics(GroupBuilder.DEFAULT_GROUP);
	}

	public Metrics newMetrics(Group group) {
		if (outOfScope) {
			throw new IllegalStateException("newMetrics() called on a collector which has " + "gone out of scope");
		}
		return primaryMetrics.newMetrics(group);
	}

	private static class EnsuredCloseMetrics extends Metrics {
		private final MetricsHolder manager;
		private final Metrics delegate;
		private volatile boolean isClosed = false;

		public EnsuredCloseMetrics(MetricsHolder manager, Metrics delegate) {
			super();
			this.manager = manager;
			this.delegate = delegate;
		}

		@Override
		public void addCount(String name, double value, Unit<?> unit, int repeat) {
			if (isClosed)
				return;
			delegate.addCount(name, value, unit, repeat);
		}

		@Override
		public void addDate(String name, double value) {
			if (isClosed)
				return;
			delegate.addDate(name, value);
		}

		@Override
		public void addLevel(String name, double value, Unit<?> unit, int repeat) {
			if (isClosed)
				return;
			delegate.addLevel(name, value, unit, repeat);
		}

		@Override
		public void addProperty(String name, CharSequence value) {
			if (isClosed)
				return;
			delegate.addProperty(name, value);
		}

		@Override
		public void addTime(String name, double value, Unit<?> unit, int repeat) {
			if (isClosed)
				return;
			delegate.addTime(name, value, unit, repeat);
		}

		@SuppressWarnings("deprecation")
		@Override
		public MetricsFactory getMetricsFactory() {
			return new EnsuredCloseMetricsFactory(manager, delegate.getMetricsFactory());
		}

		@Override
		public synchronized Metrics newMetrics(Group group) {
			if (isClosed) {
				throw new IllegalStateException("newMetrics() called after close()");
			}

			EnsuredCloseMetrics newOne = new EnsuredCloseMetrics(manager, delegate.newMetrics(group));

			manager.registerMetrics(newOne);
			return newOne;
		}

		@Override
		public void close() {

			synchronized (this) {
				if (isClosed)
					return;
				isClosed = true;
			}
			delegate.close();
		}
	}

	private static class EnsuredCloseMetricsFactory implements MetricsFactory {
		final private MetricsHolder manager;
		final private MetricsFactory delegate;

		public EnsuredCloseMetricsFactory(MetricsHolder manager, MetricsFactory delegate) {
			this.manager = manager;
			this.delegate = delegate;
		}

		@Override
		public Metrics newMetrics() {
			EnsuredCloseMetrics newOne = new EnsuredCloseMetrics(manager, delegate.newMetrics());
			manager.registerMetrics(newOne);
			return newOne;
		}
	}
}
