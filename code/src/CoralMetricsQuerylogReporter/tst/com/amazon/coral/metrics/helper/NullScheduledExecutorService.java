package com.amazon.coral.metrics.helper;

import java.util.concurrent.*;
import java.util.*;

public class NullScheduledExecutorService extends AbstractExecutorService implements ScheduledExecutorService {

  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return new NullScheduledFuture<V>();
  }

  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return new NullScheduledFuture<Object>();
  }

  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return new NullScheduledFuture<Object>();
  }

  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return new NullScheduledFuture<Object>();
  }

  public boolean isTerminated() { return false; }

  public boolean isShutdown() { return false; }

  public void shutdown() {}

  @SuppressWarnings("unchecked")
  public List<Runnable> shutdownNow() { return Collections.EMPTY_LIST; }

  public void execute(Runnable command) {
  }

  public boolean awaitTermination(long timeout,TimeUnit unit)
    throws InterruptedException { return false; }

  private static class NullScheduledFuture<T> implements ScheduledFuture<T> {
    public long getDelay(TimeUnit unit) { return 0; }
    public boolean cancel(boolean flag) { return false; }
    public boolean isCancelled() { return false; }
    public boolean isDone() { return false; }
    public T get() { return null; }
    public T get(long timeout, TimeUnit unit) { return null; }
    public int compareTo(Delayed o) { return 0; }
  }

}
