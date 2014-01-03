package com.amazon.fba.mic.metrics;

import com.amazon.coral.community.metrics.MetricsProvider;
import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.Metrics;

/**
 * A simple MetricsProvider can support multi-threads.
 * @author wdong
 *
 */
public class SimpleMetricsProvider implements MetricsProvider {
	
	private MetricsContext metricsContext;
	
	public void setMetricsContext(final MetricsContext metricsContext) {
		this.metricsContext = metricsContext;
	}

	@Override
	public Metrics getMetrics() {
		MetricsHolder holder = this.metricsContext.getMetricsHolderForThread();
		if(holder != null) {
			return holder.newMetrics();
		} else {
			return null;
		}
	}

	@Override
	public Metrics getMetrics(Group group) {
		MetricsHolder holder = this.metricsContext.getMetricsHolderForThread();
		if(holder != null) {
			return holder.newMetrics(group);
		} else {
			return null;
		}
	}

}
