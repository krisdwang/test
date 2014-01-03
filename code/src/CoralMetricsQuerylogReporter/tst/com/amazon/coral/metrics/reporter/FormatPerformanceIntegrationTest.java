package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.Date;
import java.util.concurrent.*;

public class FormatPerformanceIntegrationTest {

  private static final TimeZone timeZone = TimeZone.getTimeZone("UTC");
  private static final int N = 1000000;
  private static final long now = System.currentTimeMillis();

  // SimpleDate profiling task
  private static final Runnable simpleDateISO = new Runnable() {

    // This is actually cheating a bit, this class isn't thread safe
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    public void run() {
      sdf.setTimeZone(timeZone);
      for(int i = 0; i < N; i++)
        sdf.format(new Date(now));
    }

    @Override
    public String toString() {
      return sdf.getClass().getName();
    }

  };

  // DateFormat profiling task
  private static final Runnable isoDate = new Runnable() {

    // This is actually cheating a bit, this class isn't thread safe
    private final ISO8601Format format = new ISO8601Format();
    private final ThreadLocalCalendarFactory calendarFactory = new ThreadLocalCalendarFactory(timeZone);

    public void run() {
      for(int i = 0; i < N; i++)
        format.format(new StringBuilder(),now,calendarFactory);
    }

    @Override
    public String toString() {
      return format.getClass().getName();
    }

  };

  // DateFormat profiling task
  private static final Runnable endDate = new Runnable() {

    // This is actually cheating a bit, this class isn't thread safe
    private final EndTimeFormat format = new EndTimeFormat();
    private final ThreadLocalCalendarFactory calendarFactory = new ThreadLocalCalendarFactory(timeZone);

    public void run() {
      for(int i = 0; i < N; i++)
        format.format(new StringBuilder(),now,calendarFactory);
    }

    @Override
    public String toString() {
      return format.getClass().getName();
    }

  };

  // DateFormat profiling task
  private static final Runnable cachedDate = new Runnable() {

    // This is actually cheating a bit, this class isn't thread safe
    private final CachedDateFormat format = new CachedDateFormat(new ISO8601Format());
    private final ThreadLocalCalendarFactory calendarFactory = new ThreadLocalCalendarFactory(timeZone);

    public void run() {
      for(int i = 0; i < N; i++)
        format.format(new StringBuilder(),now,calendarFactory);
    }

    @Override
    public String toString() {
      return format.getClass().getName();
    }

  };

  // DateFormat profiling task
  private static final Runnable cachedDate2 = new Runnable() {

    // This is actually cheating a bit, this class isn't thread safe
    private final CachedDateFormat format = new CachedDateFormat(new EndTimeFormat());
    private final ThreadLocalCalendarFactory calendarFactory = new ThreadLocalCalendarFactory(timeZone);

    public void run() {
      for(int i = 0; i < N; i++)
        format.format(new StringBuilder(),now,calendarFactory);
    }

    @Override
    public String toString() {
      return format.getClass().getName();
    }

  };

  @Test
  public void simpleDateISOFormat1() {
    run(simpleDateISO, 1);
  }

  @Test
  public void simpleDateISOFormat10() {
    run(simpleDateISO, 10);
  }

  @Test
  public void isoFormat1() {
    run(isoDate, 1);
  }

  @Test
  public void isoFormat10() {
    run(isoDate, 10);
  }

  @Test
  public void cachedFormat1() {
    run(cachedDate, 1);
  }

  @Test
  public void cachedFormat10() {
    run(cachedDate, 10);
  }

  @Test
  public void endFormat1() {
    run(endDate, 1);
  }

  @Test
  public void endFormat10() {
    run(endDate, 10);
  }

  @Test
  public void cached2Format1() {
    run(cachedDate2, 1);
  }

  @Test
  public void cached2Format10() {
    run(cachedDate2, 10);
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
