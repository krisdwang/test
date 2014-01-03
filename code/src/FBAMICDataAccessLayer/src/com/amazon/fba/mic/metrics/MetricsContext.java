package com.amazon.fba.mic.metrics;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;

/**
 * 
 * @author wdong
 * 
 */
public class MetricsContext {

	private static final Logger logger = Logger.getLogger(MetricsContext.class);

	private static class MetricsRef {
		private final MetricsHolder holder;
		private AtomicInteger count = new AtomicInteger(1);

		public MetricsRef(MetricsHolder holder) {
			this.holder = holder;
		}

		public MetricsHolder getHolder() {
			return this.holder;
		}

		public void refForThread() {
			this.count.addAndGet(1);
		}

		public boolean unRefForThread() {
			if (count.intValue() == 0) {
				throw new IllegalStateException("");
			}
			this.count.decrementAndGet();
			return this.count.intValue() == 0;
		}

	}

	private MetricsFactory metricsFactory;

	private ThreadLocal<MetricsRef> metricsRefThreadLocalHolder = new ThreadLocal<MetricsRef>();

	public void setMetricsFactory(MetricsFactory metricsFactory) {
		this.metricsFactory = metricsFactory;
	}

	public MetricsHolder newMetricsHolderForThread() {

		if (this.metricsFactory == null) {
			throw new IllegalStateException("MetricsFactory for MetricsContext can't be empty, current thread is "
					+ Thread.currentThread(), new Throwable());
		}

		MetricsHolder holder = new MetricsHolder(this.metricsFactory.newMetrics());

		this.metricsRefThreadLocalHolder.set(new MetricsRef(holder));

		return holder;
	}

	public void setMetricsHolderForThread(MetricsHolder source) {
		if (null == source) {
			throw new IllegalArgumentException("MetricsHolder is null");
		}

		MetricsRef ref = this.metricsRefThreadLocalHolder.get();

		if (ref != null && ref.getHolder() == source) {
			ref.refForThread();
		} else {
			if (ref != null) {
				logger.error("setMetricsHolderForThread() called before closing "
						+ "the previous holder:  Implicitly closing to " + "preserve metrics:" + Thread.currentThread()
						+ "\n", new Throwable());
				ref.getHolder().unref();
			}
			source.ref();
			this.metricsRefThreadLocalHolder.set(new MetricsRef(source));
		}
	}

	public void clearMetricsHolderForThread() {
		MetricsRef metricsRef = this.metricsRefThreadLocalHolder.get();
		if (metricsRef != null) {
			if (metricsRef.unRefForThread()) {
				metricsRef.getHolder().unref();
				this.metricsRefThreadLocalHolder.remove();
			}
		} else {
			logger.error("clearMetricsHolderForThread() called more than once:" + Thread.currentThread(),
					new Throwable());
		}
	}

	public MetricsHolder getMetricsHolderForThread() {
		MetricsRef metricsRef = this.metricsRefThreadLocalHolder.get();
		return metricsRef == null ? null : metricsRef.getHolder();
	}

	public Metrics getMetrics() {
		MetricsRef metricsRef = this.metricsRefThreadLocalHolder.get();
		return metricsRef == null ? null : metricsRef.getHolder().newMetrics();
	}
}
