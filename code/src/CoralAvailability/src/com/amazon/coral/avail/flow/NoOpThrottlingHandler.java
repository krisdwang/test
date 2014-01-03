// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.flow;

import com.amazon.coral.service.UserHandler;
import com.amazon.coral.service.HandlerContext;

/**
 * A wrapper around a {@code ThrottlingHandler} that allows throttling
 * logic to run in no-op mode. Its purpose is to force the emission of
 * throttling metrics without actually throttling anything.
 * <p>
 * @author Sam Young &lt;sayo@amazon.com&gt;
 */
public class NoOpThrottlingHandler extends UserHandler {
  private final ThrottlingHandler handler;

  public NoOpThrottlingHandler(ThrottlingHandler handler) {
    if (handler == null)
      throw new IllegalArgumentException();
    this.handler = handler;
  }

  @Override
  public void before(HandlerContext ctx) {
    try {
      handler.before(ctx);
    } catch (com.amazon.coral.availability.ThrottlingException e) {
      // Nothing -- We just want metrics to be emitted.
      // This will happen in the delegate handler.
    }
  }
}
