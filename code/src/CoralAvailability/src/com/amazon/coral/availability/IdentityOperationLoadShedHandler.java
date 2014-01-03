// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import static com.amazon.coral.availability.IdentityKeyBuilder.DEFAULT_ATTRIBUTES;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import com.amazon.coral.throttle.api.Throttler;

/** @Deprecated prefer com.amazon.coral.avail.handlers.LoadShedHandler */
@Deprecated
public class IdentityOperationLoadShedHandler extends LoadShedHandler {
  @Deprecated
  public IdentityOperationLoadShedHandler(int capacity, ThrottlingStrategy strategy) {
    this(DEFAULT_ATTRIBUTES, capacity, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public IdentityOperationLoadShedHandler(int capacity, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(DEFAULT_ATTRIBUTES, capacity, strategy, exemptions);
  }

  @Deprecated
  public IdentityOperationLoadShedHandler(List<String> keys, int capacity, ThrottlingStrategy strategy) {
    this(keys, capacity, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public IdentityOperationLoadShedHandler(List<String> keys, int capacity, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(keys, capacity, new ThrottlerAdapter(strategy), exemptions);
  }

  IdentityOperationLoadShedHandler(List<String> keys, int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    super(getKeyBuilder(keys), capacity, throttler, exemptions);
  }

  private static KeyBuilder getKeyBuilder(List<String> keys) {
    ArrayList<KeyBuilder> builders = new ArrayList<KeyBuilder>(2);
    builders.add(new IdentityKeyBuilder(keys));
    builders.add(new OperationKeyBuilder());
    return new CrossProductKeyBuilder(builders, ",");
  }

  static IdentityOperationLoadShedHandler build(LoadShedHandler.Builder cfg) {
    List<String> keys = (null!=cfg.keys) ? cfg.keys : DEFAULT_ATTRIBUTES;
    return new IdentityOperationLoadShedHandler(keys, cfg.capacity, cfg.strat, cfg.exemptions);
  }
}
