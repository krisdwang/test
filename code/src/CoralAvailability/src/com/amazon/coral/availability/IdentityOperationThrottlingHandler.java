package com.amazon.coral.availability;

import static com.amazon.coral.availability.IdentityKeyBuilder.DEFAULT_ATTRIBUTES;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.amazon.coral.throttle.api.Throttler;

/**
 * The {@code IdentityOperationThrottlingHandler} can be used on the server-side to
 * throttle incoming requests based on the combination of identity keys and
 * operation name. Keys are generated of the form:
 * IdentityKey:IdentityValue,Operation:ServiceName/OperationName
 * e.g.
 * aws-account:0NEXY48J13KN38NDY90,Operation:WeatherService/GetWeather
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/Throttling">https://w.amazon.com/?Coral/Manual/Availability/Throttling</a>
 * <p>
 * @Deprecated prefer com.amazon.coral.avail.handlers.ThrottlingHandler
 */
@Deprecated
public class IdentityOperationThrottlingHandler extends ThrottlingHandler {

  private static final String THROTTLE_KEY_PREFIX = "ID_OP:";

  @Deprecated
  public IdentityOperationThrottlingHandler(ThrottlingStrategy strategy) {
    this(DEFAULT_ATTRIBUTES, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public IdentityOperationThrottlingHandler(ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(DEFAULT_ATTRIBUTES, strategy, exemptions);
  }

  @Deprecated
  public IdentityOperationThrottlingHandler(List<String> keys, ThrottlingStrategy strategy) {
    this(keys, strategy, Collections.<CharSequence>emptyList());
  }

  /**
   * Prefixes the throttle keys with the handler-unique prefix string.
   * This makes it possible to use CoralThrottle with separate configuration for different
   * throttling handlers, without the "longest prefix match" config lookup algorithm
   * causing unexpected behavior
   *
   * For example, passing in "true" will result in throttle keys similar to:
   * ID_OP:aws-account:1234234,Operation:BigBirdService/GetItems
   */
  @Deprecated
  public IdentityOperationThrottlingHandler(List<String> keys, boolean usePrefix, ThrottlingStrategy strategy) {
    this(keys, usePrefix ? THROTTLE_KEY_PREFIX : "", new ThrottlerAdapter(strategy), Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public IdentityOperationThrottlingHandler(List<String> keys, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(keys, "", new ThrottlerAdapter(strategy), exemptions);
  }

  IdentityOperationThrottlingHandler(List<String> keys, String prefix, Throttler throttler, Collection<CharSequence> exemptions) {
    super(PrefixKeyBuilder.maybePrefix(getKeyBuilder(keys), prefix, ""), throttler, exemptions);
  }

  private static KeyBuilder getKeyBuilder(List<String> keys) {
    ArrayList<KeyBuilder> builders = new ArrayList<KeyBuilder>(2);
    builders.add(new IdentityKeyBuilder(keys));
    builders.add(new OperationKeyBuilder());
    return new CrossProductKeyBuilder(builders, ",");
  }

  static IdentityOperationThrottlingHandler build(ThrottlingHandler.Builder cfg) {
    List<String> keys = (null != cfg.keys) ? cfg.keys : DEFAULT_ATTRIBUTES;
    String prefix = cfg.usePrefix ? THROTTLE_KEY_PREFIX : "";
    return new IdentityOperationThrottlingHandler(keys, prefix, cfg.throttler, cfg.exemptions);
  }

}
