// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import java.util.Collection;
import java.util.Collections;
import com.amazon.coral.throttle.api.Throttler;

/** @Deprecated prefer com.amazon.coral.avail.handlers.LoadShedHandler */
@Deprecated
public class OperationLoadShedHandler extends LoadShedHandler {
  @Deprecated
  public OperationLoadShedHandler(int capacity, ThrottlingStrategy strategy) {
    this(capacity, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public OperationLoadShedHandler(int capacity, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(capacity, new ThrottlerAdapter(strategy), exemptions);
  }

  OperationLoadShedHandler(int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    super(new OperationKeyBuilder(), capacity, throttler, exemptions);
  }

  static OperationLoadShedHandler build(LoadShedHandler.Builder cfg) {
    return new OperationLoadShedHandler(cfg.capacity, cfg.strat, cfg.exemptions);
  }
}

