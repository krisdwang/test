package com.amazon.coral.metrics.reporter;

import java.io.PrintWriter;
import java.io.Writer;
import java.io.OutputStream;
import java.util.TimeZone;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.amazon.coral.metrics.simple.AggregatingReporterFactory;

/**
 */
public class QuerylogReporterFactory implements ReporterFactory {

  private final Writer writer;
  private final Lock writerLock = new ReentrantLock();
  private final QuerylogFormat format;
  private final ReporterFactory internalFactory;
  private final boolean optimisticSampling;

  public QuerylogReporterFactory(OutputStream out) {
    this(newPrintWriter(out));
  }

  public QuerylogReporterFactory(Writer writer) {
    this(writer, TimeZone.getTimeZone("UTC"), false, false);
  }

  public QuerylogReporterFactory(Writer writer, boolean optimisticSampling, boolean offloadingEnabled) {
    this(writer, TimeZone.getTimeZone("UTC"), optimisticSampling, offloadingEnabled);
  }

  public QuerylogReporterFactory(Writer writer, TimeZone timeZone) {
    this(writer, timeZone, false, false);
  }

  public QuerylogReporterFactory(Writer writer, TimeZone timeZone, boolean optimisticSampling) {
    this(writer, timeZone, optimisticSampling, false);
  }

  public QuerylogReporterFactory(Writer writer, TimeZone timeZone, boolean optimisticSampling, boolean offloadingEnabled) {
    this(writer, timeZone, optimisticSampling, offloadingEnabled, -1, -1);
  }
  
  public QuerylogReporterFactory(Writer writer, TimeZone timeZone, boolean optimisticSampling, boolean offloadingEnabled, int taskQueueSize, int workerThreads) {
    if(writer == null)
      throw new IllegalArgumentException();
    if(timeZone == null)
      throw new IllegalArgumentException();
    this.writer = writer;
    this.format = new QuerylogFormat(new ThreadLocalCalendarFactory(timeZone));
    // NOTE: This ReporterFactory could be removed when monitoring is able to
    // NOTE: support the Remote metric
    ReporterFactory f = new InternalReporterFactory();
    if(offloadingEnabled) {
      if(taskQueueSize > 0)
        f = new OffloadingReporterFactory(f, taskQueueSize, workerThreads);
      else
        f = new OffloadingReporterFactory(f);
    }
    this.internalFactory = new OldBsfMetricReporterFactory(new AggregatingReporterFactory(f));
    this.optimisticSampling = optimisticSampling;
  }

  public Reporter newReporter() {
    return internalFactory.newReporter();
  }

  private class InternalReporterFactory implements ReporterFactory {
    public Reporter newReporter() {
      return new QuerylogReporter(writer, writerLock, format, optimisticSampling);
    }
  }

  private static PrintWriter newPrintWriter(OutputStream out) {
    if(out == null)
      throw new IllegalArgumentException();
    return new PrintWriter(out);
  }

}
