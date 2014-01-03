package com.amazon.coral.metrics.helper;

import com.amazon.coral.metrics.Metrics;

/**
 * Creates {@link Metrics} that are thread-safe via synchronization on their
 * methods. This is useful in cases where {@link Metrics#newMetrics()} objects
 * need to be {@link Metrics#close()}d, but the delegate thread cannot be relied
 * upon to do so or the thread cannot be guaranteed to terminate in a timely
 * manner. When a delegate thread can potentially get stuck or terminate slowly,
 * the main thread can safely call {@link Metrics#close()} and record metrics.
 */
// https://issues.amazon.com/SF-588
public class MetricsSynchronizer {

    /**
     * Returns a synchronized (thread-safe) metrics backed by the specified
     * Metrics. In order to guarantee serial access, it is critical that all
     * access to the backing metrics is accomplished through the returned
     * metrics.
     * <p>
     * Synchronization concerns:
     * </p>
     * <p>
     * If necessary to perform multiple operations atomically, the caller may
     * explicitly synchronize on an instance of this class.
     * </p>
     * <p>
     * Usage note:
     * </p>
     * <p>
     * If a thread calls {@link Metrics#close()}, then the metrics instance is
     * immediately closed across all threads. with the ordinary behavior of the
     * backing Metrics instance. For uses that require Metrics objects to be
     * both thread-safe and closure-tolerant, the user can put their Metrics
     * instance in a {@link ClosureTolerantMetricsContainer} before calling
     * {@link #synchronize(Metrics)}.
     * </p>
     * 
     * @param metrics
     *            the metrics to be wrapped in a synchronized Metrics
     * @return a synchronized view of the specified metrics.
     */
    public static Metrics synchronize(final Metrics metrics) {
        return new SynchronizedMetrics(metrics);
    }

    private MetricsSynchronizer() {};
}
