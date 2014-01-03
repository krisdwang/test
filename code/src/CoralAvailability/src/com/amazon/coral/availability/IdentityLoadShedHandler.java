// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import static com.amazon.coral.availability.IdentityKeyBuilder.DEFAULT_ATTRIBUTES;

import java.util.List;
import java.util.Collection;
import java.util.Collections;
import com.amazon.coral.throttle.api.Throttler;

/** @Deprecated prefer com.amazon.coral.avail.handlers.LoadShedHandler */
@Deprecated
public class IdentityLoadShedHandler extends LoadShedHandler {
  @Deprecated
  public IdentityLoadShedHandler(int capacity, ThrottlingStrategy strategy) {
    this(DEFAULT_ATTRIBUTES, capacity, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public IdentityLoadShedHandler(int capacity, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(DEFAULT_ATTRIBUTES, capacity, strategy, exemptions);
  }

  @Deprecated
  public IdentityLoadShedHandler(List<String> keys, int capacity, ThrottlingStrategy strategy) {
    this(keys, capacity, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public IdentityLoadShedHandler(List<String> keys, int capacity, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(keys, capacity, new ThrottlerAdapter(strategy), exemptions);
  }

  IdentityLoadShedHandler(List<String> keys, int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    super(new IdentityKeyBuilder(keys), capacity, throttler, exemptions);
  }

  static IdentityLoadShedHandler build(LoadShedHandler.Builder cfg) {
    List<String> keys = (null!=cfg.keys) ? cfg.keys : DEFAULT_ATTRIBUTES;
    return new IdentityLoadShedHandler(keys, cfg.capacity, cfg.strat, cfg.exemptions);
  }
}
