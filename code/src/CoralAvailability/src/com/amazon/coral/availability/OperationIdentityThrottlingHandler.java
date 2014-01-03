package com.amazon.coral.availability;

import static com.amazon.coral.availability.IdentityKeyBuilder.DEFAULT_ATTRIBUTES;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.amazon.coral.throttle.api.Throttler;

/**
 * The {@code OperationIdentityThrottlingHandler} can be used on the
 * server-side to throttle incoming requests based on the combination of
 * operation name and identity keys.  Keys are generated of the form:
 * Operation:ServiceName/OperationName,IdentityKey:IdentityValue
 * e.g.
 * Operation:WeatherService/GetWeather,aws-account:0NEXY48J13KN38NDY90
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/Throttling">https://w.amazon.com/?Coral/Manual/Availability/Throttling</a>
 * <p>
 *  @Deprecated prefer com.amazon.coral.avail.handlers.ThrottlingShedHandler
 */
@Deprecated
public class OperationIdentityThrottlingHandler extends ThrottlingHandler {

  private static final String THROTTLE_KEY_PREFIX = "OP_ID:";

  @Deprecated
  public OperationIdentityThrottlingHandler(ThrottlingStrategy strategy) {
    this(DEFAULT_ATTRIBUTES, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public OperationIdentityThrottlingHandler(ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(DEFAULT_ATTRIBUTES, strategy, exemptions);
  }

  @Deprecated
  public OperationIdentityThrottlingHandler(List<String> keys, ThrottlingStrategy strategy) {
    this(keys, "", new ThrottlerAdapter(strategy), Collections.<CharSequence>emptyList());
  }

  /**
   * Prefixes the throttle keys with a handler-unique prefix string.
   * This makes it possible to use CoralThrottle with separate configuration for different
   * throttling handlers, without the "longest prefix match" config lookup algorithm
   * causing unexpected behavior
   *
   * For example, passing in the "true" will result in throttle keys similar to:
   * OP_ID:Operation:GetWeather,aws-account:1234234
   *
   * @param strategy
   * @param prefix
   */
  @Deprecated
  public OperationIdentityThrottlingHandler(List<String> keys, boolean usePrefix, ThrottlingStrategy strategy) {
    this(keys, usePrefix ? THROTTLE_KEY_PREFIX : "", new ThrottlerAdapter(strategy), Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public OperationIdentityThrottlingHandler(List<String> keys, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(keys, "", new ThrottlerAdapter(strategy), exemptions);
  }

  OperationIdentityThrottlingHandler(List<String> keys, String prefix, Throttler throttler, Collection<CharSequence> exemptions) {
    super(getKeyBuilder(keys, prefix), throttler, exemptions);
  }

  private static KeyBuilder getKeyBuilder(List<String> keys, String prefix) {
    ArrayList<KeyBuilder> builders = new ArrayList<KeyBuilder>(2);
    builders.add(new OperationKeyBuilder());
    builders.add(new IdentityKeyBuilder(keys));
    return PrefixKeyBuilder.maybePrefix(new CrossProductKeyBuilder(builders, ","), prefix, "");
  }

  static OperationIdentityThrottlingHandler build(ThrottlingHandler.Builder cfg) {
    List<String> keys = (null != cfg.keys) ? cfg.keys : DEFAULT_ATTRIBUTES;
    String prefix = cfg.usePrefix ? THROTTLE_KEY_PREFIX : "";
    return new OperationIdentityThrottlingHandler(keys, prefix, cfg.throttler, cfg.exemptions);
  }

}
