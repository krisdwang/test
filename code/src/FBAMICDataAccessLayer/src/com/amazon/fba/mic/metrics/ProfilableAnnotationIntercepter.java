package com.amazon.fba.mic.metrics;

import java.lang.annotation.Annotation;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.ReflectiveMethodInvocation;
import org.springframework.aop.support.AopUtils;

import com.amazon.coral.metrics.Metrics;

public class ProfilableAnnotationIntercepter<T> implements MethodInterceptor {

	private MetricsContext metricsContext;

	public ProfilableAnnotationIntercepter(final MetricsContext metricsContext) {
		this.metricsContext = metricsContext;
	}

	public Object invoke(MethodInvocation invocation) throws Throwable {
		MetricsHolder metricsHolder = this.metricsContext.getMetricsHolderForThread();
		if (metricsHolder == null) {
			return collectMetrics(invocation);
		}
		return collectMetrics(invocation, metricsHolder.newMetrics());
	}

	private Object collectMetrics(MethodInvocation invocation) throws Throwable {
		Metrics metrics = metricsContext.newMetricsHolderForThread().newMetrics();
		// TODO: prototype, need to be confirmed which should be added to
		// metrics
		metrics.addProperty("Method", invocation.getMethod().getName());
		long start = System.nanoTime();
		try {
			return invocation.proceed();
		} catch (Exception ex) {
			metrics.addCount("Exception", 1, Unit.ONE);
			metrics.addProperty("ExceptionType", ex.getClass().getName());
			// Re-throw
			throw ex;
		} finally {
			long stop = System.nanoTime();
			metrics.addDate("StartTime", start);
			metrics.addDate("EndTime", stop);
			metrics.addTime("Time", stop - start, SI.NANO(SI.SECOND));
			metrics.close();
			metricsContext.clearMetricsHolderForThread();
		}
	}

	private Object collectMetrics(MethodInvocation invocation, Metrics metrics) throws Throwable {
		long start = System.nanoTime();
		try {
			return invocation.proceed();
		} finally {
			// Add timing information.
			long stop = System.nanoTime();
			String baseMetricName = invocation.getThis().getClass().getName() + '.' + invocation.getMethod().getName();
			metrics.addTime(baseMetricName + "Latency:", stop - start, SI.NANO(SI.SECOND));
			metrics.addCount(baseMetricName + " Calls ", 1, Unit.ONE);
		}
	}

}
