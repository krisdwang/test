// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import static com.amazon.coral.availability.IdentityKeyBuilder.DEFAULT_ATTRIBUTES;

import java.util.List;
import java.util.Collection;
import java.util.Collections;

import com.amazon.coral.throttle.api.Throttler;

/**
 * The {@code IdentityThrottlingHandler} is a generic {@code Handler} that can
 * be used on the server-side to throttle any incoming request based on any
 * piece of {@code Identity} information about that request. The default
 * identity attributes used are aws-account, aws-access-key, and http-remote-
 * address. A list of specific keys to use can be passed to the constructor.
 * The actual mechanism for deciding if a key should be throttled is
 * pluggable, allowing both local and distributed versions to be provided.
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/Throttling">https://w.amazon.com/?Coral/Manual/Availability/Throttling</a>
 * <p>
 * @author Eric Crahen &lt;crahen@amazon.com&gt;
 * @version 1.0
 * @Deprecated prefer com.amazon.coral.avail.handlers.ThrottlingHandler
 */
@Deprecated
public class IdentityThrottlingHandler extends ThrottlingHandler {

  private static final String THROTTLE_KEY_PREFIX = "ID:";

  @Deprecated
  public IdentityThrottlingHandler(ThrottlingStrategy strategy) {
    this(strategy, Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public IdentityThrottlingHandler(ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(DEFAULT_ATTRIBUTES, "", new ThrottlerAdapter(strategy), exemptions);
  }

  @Deprecated
  public IdentityThrottlingHandler(List<String> keys, ThrottlingStrategy strategy) {
    this(keys, "", new ThrottlerAdapter(strategy), Collections.<CharSequence>emptyList());
  }

  /**
   * Prefixes the throttle keys with a handler-unique prefix string.
   * This makes it possible to use CoralThrottle with separate configuration for different
   * throttling handlers, without the "longest prefix match" config lookup algorithm
   * causing unexpected behavior
   *
   * For example, passing in "true" will result in throttle keys similar to:
   * ID:aws-account:1234234
   */
  @Deprecated
  public IdentityThrottlingHandler(List<String> keys, boolean usePrefix, ThrottlingStrategy strategy) {
    this(keys, usePrefix ? THROTTLE_KEY_PREFIX : "", new ThrottlerAdapter(strategy), Collections.<CharSequence>emptyList());
  }

  @Deprecated
  public IdentityThrottlingHandler(List<String> keys, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(keys, "", new ThrottlerAdapter(strategy), exemptions);
  }

  IdentityThrottlingHandler(List<String> keys, String prefix, Throttler throttler, Collection<CharSequence> exemptions) {
    super(PrefixKeyBuilder.maybePrefix(new IdentityKeyBuilder(keys), prefix, ""), throttler, exemptions);
  }

  static IdentityThrottlingHandler build(ThrottlingHandler.Builder cfg) {
    List<String> keys = (null != cfg.keys) ? cfg.keys : DEFAULT_ATTRIBUTES;
    String prefix = cfg.usePrefix ? THROTTLE_KEY_PREFIX : "";
    return new IdentityThrottlingHandler(keys, prefix, cfg.throttler, cfg.exemptions);
  }

}
