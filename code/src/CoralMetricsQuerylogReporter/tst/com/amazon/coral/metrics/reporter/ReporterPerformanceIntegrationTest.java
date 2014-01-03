package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.helper.*;
import com.amazon.coral.metrics.*;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class ReporterPerformanceIntegrationTest {

  private static final int N = 100000;
  private static final long now = System.currentTimeMillis();

  private static ReporterFactory newNullReporterFactory() {
    return new NullReporterFactory();
  }

  private static ReporterFactory newFileReporterFactory() {
    try {
      File f = new File("build/private", "reporter.perf" + System.currentTimeMillis());
      f.getParentFile().mkdirs();
      f.deleteOnExit();
      Writer writer = new FileWriter(f);
      return new QuerylogReporterFactory(writer);
    } catch(Throwable t) { throw new Error(t); }
  }

  private static ReporterFactory newBufferedFileReporterFactory() {
    try {
      File f = new File("build/private", "reporter.perf" + System.currentTimeMillis());
      f.getParentFile().mkdirs();
      f.deleteOnExit();
      QuerylogHelper h = new QuerylogHelper();
      h.setFilename(f.getPath());
      h.setRecordsToBuffer(100);
      return h;
    } catch(Throwable t) { throw new Error(t); }
  }

  private static ReporterFactory newFixedSampledFileReporterFactory() {
    try {
      File f = new File("build/private", "reporter.perf" + System.currentTimeMillis());
      f.getParentFile().mkdirs();
      f.deleteOnExit();
      QuerylogHelper h = new QuerylogHelper();
      h.setFilename(f.getPath());
      h.setSamplingRate(0.25);
      return h;
    } catch(Throwable t) { throw new Error(t); }
  }

  private static ReporterFactory newOptSampledFileReporterFactory() {
    try {
      File f = new File("build/private", "reporter.perf" + System.currentTimeMillis());
      f.getParentFile().mkdirs();
      f.deleteOnExit();
      QuerylogHelper h = new QuerylogHelper();
      h.setFilename(f.getPath());
      h.setOptimisticSampling(true);
      return h;
    } catch(Throwable t) { throw new Error(t); }
  }

  private static final Runnable nullTask = new Runnable() {
    final ReporterFactory f = newNullReporterFactory();
    @Override
    public void run() {
      final Reporter reporter = f.newReporter();
      for(int i = 0; i < N; i++) {
        reporter.beginReport();
        reporter.endReport();
      }
    }
    public String toString() {
      return "null test";
    }
  };

  private static final Runnable fileTask = new Runnable() {
    final ReporterFactory f = newFileReporterFactory();
    @Override
    public void run() {
      final Reporter reporter = f.newReporter();
      for(int i = 0; i < N; i++) {
        reporter.beginReport();
        reporter.endReport();
      }
    }
    public String toString() {
      return "file test (no buffer)";
    }
  };

  private static final Runnable bufferFileTask = new Runnable() {
    final ReporterFactory f = newBufferedFileReporterFactory();
    @Override
    public void run() {
      final Reporter reporter = f.newReporter();
      for(int i = 0; i < N; i++) {
        reporter.beginReport();
        reporter.endReport();
      }
    }
    public String toString() {
      return "file test (buffering 100x)";
    }
  };

  private static final Runnable fixedSampleFileTask = new Runnable() {
    final ReporterFactory f = newFixedSampledFileReporterFactory();
    @Override
    public void run() {
      final Reporter reporter = f.newReporter();
      for(int i = 0; i < N; i++) {
        reporter.beginReport();
        reporter.endReport();
      }
    }
    public String toString() {
      return "file test (fixed sampling .25)";
    }
  };


  private static final Runnable optSampleFileTask = new Runnable() {
    final ReporterFactory f = newOptSampledFileReporterFactory();
    @Override
    public void run() {
      final Reporter reporter = f.newReporter();
      for(int i = 0; i < N; i++) {
        reporter.beginReport();
        reporter.endReport();
      }
    }
    public String toString() {
      return "file test (optimistic sampling)";
    }
  };

  @Test
  public void nullTask1()
    throws Throwable {
    run(nullTask, 1);
  }

  @Test
  public void nullTask10()
    throws Throwable {
    run(nullTask, 10);
  }

  @Test
  public void fileTask1()
    throws Throwable {
    run(fileTask, 1);
  }

  @Test
  public void fileTask10()
    throws Throwable {
    run(fileTask, 10);
  }

  @Test
  public void bufferFileTask1()
    throws Throwable {
    run(bufferFileTask, 1);
  }

  @Test
  public void bufferFileTask10()
    throws Throwable {
    run(bufferFileTask, 10);
  }

  @Test
  public void optSampleFileTask1()
    throws Throwable {
    run(optSampleFileTask, 1);
  }

  @Test
  public void optSampleFileTask10()
    throws Throwable {
    run(optSampleFileTask, 10);
  }

  // Helpers for timing the tests accurately
  private static class TimedRunnable implements Runnable {

    private final CyclicBarrier barrier;
    private final Runnable r;
    private long time;

    TimedRunnable(CyclicBarrier barrier, Runnable r) {
      this.barrier = barrier;
      this.r = r;
    }

    public void run() {
      try {
        barrier.await();
      } catch(Throwable t) { throw new Error(t); }
      long start = System.currentTimeMillis();
      try {
        r.run();
      } finally {
        time = (System.currentTimeMillis() - start);
      }
    }

    long time() { return time; }

  }

  private long run(Runnable r, int threads) {

    long time = 0;

    try {

      CyclicBarrier barrier = new CyclicBarrier(threads);
      Thread[] t = new Thread[threads];
      TimedRunnable[] task = new TimedRunnable[threads];

      for(int i = 0; i < threads; i++) {
        task[i] = new TimedRunnable(barrier, r);
        t[i] = new Thread(task[i]);
        t[i].start();
      }

      for(int i = 0; i < threads; i++) {
        t[i].join();
        time += task[i].time();
      }

    } catch(Throwable t) { throw new Error(t); }

    System.out.println(r.toString() + " " + N + " iterations, " + threads + " threads, " + time + "ms");
    return time;

  }

}
