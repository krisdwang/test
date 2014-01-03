package com.amazon.coral.availability;

import com.amazon.coral.service.AbstractHandler;
import com.amazon.coral.service.Job;

/**
 * A wrapper around a {@code ThrottlingHandler} that allows throttling
 * logic to run in no-op mode. Its purpose is to force the emission of
 * throttling metrics without actually throttling anything.
 * <p>
 * Copyright (c) 2010 Amazon.com. All rights reserved.
 * @author Sam Young &lt;sayo@amazon.com&gt;
 * @Deprecated com.amazon.coral.availability.handlers.NoOpThrottlingHandler
 */
@Deprecated
public class NoOpThrottlingHandler extends AbstractHandler {

  private final ThrottlingHandler handler;

  public NoOpThrottlingHandler(ThrottlingHandler handler) {
    if (handler == null)
      throw new IllegalArgumentException();
    this.handler = handler;
  }

  @Override
  public void before(Job job) throws Throwable {
    try {
      handler.before(job);
    } catch (ThrottlingException e) {
      // Nothing -- We just want metrics to be emitted.
      // This will happen in the delegate handler.
    }
  }

}
