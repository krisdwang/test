package com.amazon.coral.metrics.helper;

import com.amazon.coral.metrics.Metrics;

/**
 * When clients issue asynchronous calls, the context managing the request may
 * have already decided to move on when the call completes. When this occurs,
 * the request's metrics will have likely been closed and subsequent metrics
 * operations will often result in exceptions under non-exceptional
 * circumstances. (See com.amazon.coral.metrics.DefaultMetrics).
 * 
 * https://issues.amazon.com/issues/e96c4538-69c6-4f44-bac0-39782f555848
 * 
 * @author yamanoha
 * 
 */
public class ClosureTolerantMetricsContainer extends DelegateMetrics {

  /**
   * The compatibility implementation which becomes active once the delegated
   * metrics is closed.
   */
  private ClosureTolerantCompatibilityMetrics compatibilityMetrics;

  /**
   * Constructs a new closure tolerant metrics object. See
   * {@link ClosureTolerantCompatibilityMetrics} for details on post-closure
   * behaviour.
   * 
   * @param metrics
   *          The metrics object to wrap.
   */
  public ClosureTolerantMetricsContainer(Metrics metrics) {
    super(metrics);
    this.compatibilityMetrics = new ClosureTolerantCompatibilityMetrics(metrics);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    Metrics delegatedMetrics = super.getMetrics();
    super.setMetrics(this.compatibilityMetrics);
    delegatedMetrics.close();
  }
}
