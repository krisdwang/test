// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import static com.amazon.coral.availability.IdentityKeyBuilder.DEFAULT_ATTRIBUTES;

import java.util.List;
import java.util.Collection;
import java.util.Collections;

import com.amazon.coral.throttle.api.Throttler;

/**
 * The {@code OperationThrottlingHandler} can be used on the
 * server-side to throttle incoming requests based on the operation.
 * Keys are generated of the form: Operation:ServiceName/OperationName
 * Operation:WeatherService/GetWeather,aws-account:0NEXY48J13KN38NDY90
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/Throttling">https://w.amazon.com/?Coral/Manual/Availability/Throttling</a>
 * <p>
 * @Deprecated prefer com.amazon.coral.avail.handlers.ThrottlingHandler
 */
@Deprecated
public class OperationThrottlingHandler extends ThrottlingHandler {

  private static final String THROTTLE_KEY_PREFIX = "OP:";

  @Deprecated
  public OperationThrottlingHandler(ThrottlingStrategy strategy) {
    this(DEFAULT_ATTRIBUTES, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public OperationThrottlingHandler(ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(DEFAULT_ATTRIBUTES, strategy, exemptions);
  }

  @Deprecated
  public OperationThrottlingHandler(List<String> keys, ThrottlingStrategy strategy) {
    this(keys, strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public OperationThrottlingHandler(List<String> keys, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(keys, "", new ThrottlerAdapter(strategy), exemptions);
  }

  /**
   * Creates an operation throttling handler that looks up throttle keys by service and operation,
   * prefixed by the handler-unique prefix string.  This makes it possible to use CoralThrottle with
   * separate configuration for different throttling handlers, without the "longest prefix match"
   * config lookup algorithm biting you in unpredictable ways.
   *
   * For example, passing in "true" will result in throttle keys similar to:
   * OP:Operation:BigBirdService/GetItems
   */
  @Deprecated
  public OperationThrottlingHandler(boolean usePrefix, ThrottlingStrategy strategy) {
    this(null, usePrefix ? THROTTLE_KEY_PREFIX : "", new ThrottlerAdapter(strategy), Collections.<CharSequence>emptyList());
  }

  OperationThrottlingHandler(List<String> keys, String prefix, Throttler throttler, Collection<CharSequence> exemptions) {
    super(PrefixKeyBuilder.maybePrefix(new OperationKeyBuilder(), prefix, ""), throttler, exemptions);
  }

  static OperationThrottlingHandler build(ThrottlingHandler.Builder cfg) {
    List<String> keys = (null != cfg.keys) ? cfg.keys : DEFAULT_ATTRIBUTES;
    String prefix = cfg.usePrefix ? THROTTLE_KEY_PREFIX : "";
    return new OperationThrottlingHandler(keys, prefix, cfg.throttler, cfg.exemptions);
  }

}
