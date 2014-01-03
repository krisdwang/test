package com.amazon.fba.mic.metrics;

import java.net.InetAddress;

import javax.measure.quantity.Duration;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.core.Ordered;

import com.amazon.coral.metrics.GroupBuilder;
import com.amazon.coral.metrics.Metrics;

/**
 * 
 * @author wdong
 *
 */
@Aspect
public class MetricsProfilerAdvice implements Ordered {

	private int order = 1;

	private MetricsContext metricsContext;
	
	private static final Unit<Duration> NANOSECONDS = SI.NANO(SI.SECOND);
	
	private static final String DURATION_AFFIX = ":Latency";
    
    private static final String EXECUTION_COUNT_AFFIX = ":Calls";
	
	private String hostName;
	
	public void setMetricsContext(MetricsContext metricsContext) {
		this.metricsContext = metricsContext;
	}
	
	public MetricsProfilerAdvice() throws Exception {
		// Load host name
        hostName = InetAddress.getLocalHost().getHostName();
	}

	@SuppressWarnings("unused")
	@Pointcut("@annotation(com.amazon.fba.mic.metrics.Profilable)")
	private void profilable() {
	}

	@Around("profilable()")
	public Object profile(ProceedingJoinPoint pjp) throws Throwable {
		MetricsHolder metricsHolder = this.metricsContext.getMetricsHolderForThread();
		if (metricsHolder == null) {
			return collectMetrics(pjp);
		}
		return collectMetrics(pjp, metricsHolder.newMetrics(GroupBuilder.DEFAULT_GROUP));
	}

	private Object collectMetrics(ProceedingJoinPoint pjp, Metrics metrics) throws Throwable{
		long start = System.nanoTime();
		try {
			return pjp.proceed();
		} finally {
			// Add timing information.
			long stop = System.nanoTime();
            String baseMetricName = pjp.getTarget().getClass().getName() + '.' + pjp.getSignature().getName();
            metrics.addTime(baseMetricName + DURATION_AFFIX,
                    stop - start, NANOSECONDS);
            metrics.addCount(baseMetricName + EXECUTION_COUNT_AFFIX, 1, Unit.ONE);
		}
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public int getOrder() {
		return order;
	}

	private Object collectMetrics(ProceedingJoinPoint pjp) throws Throwable {
		Metrics metrics = metricsContext.newMetricsHolderForThread().newMetrics(GroupBuilder.DEFAULT_GROUP);
		// TODO: prototype, need to be confirmed which should be added to metrics
		metrics.addProperty("Operation", pjp.getTarget().getClass().getName() + '.' + pjp.getSignature().getName());
        metrics.addProperty("Hostname", hostName);
		long start = System.nanoTime();
		try {
			return pjp.proceed();
		} catch (Exception ex) {
			metrics.addCount("Exception", 1, Unit.ONE);
			metrics.addProperty("ExceptionType", ex.getClass().getName());
			// Re-throw
			throw ex;
		} finally {
			 // Finish metrics
            long stop = System.nanoTime();
            metrics.addDate("StartTime", start);
            metrics.addDate("EndTime", stop);
            metrics.addTime("Time", stop - start, NANOSECONDS);
            // Done - publish
            metrics.close();

            // Clear the association between the metrics collector and this
            // thread.  When it is becomes clear of any threads it had been
            // associated with it will flush and clean itself up.
            metricsContext.clearMetricsHolderForThread();
		}
	}
}
