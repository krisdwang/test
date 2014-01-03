// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.flow;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.google.common.base.Preconditions;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.UserHandler;
import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.util.CharSequenceHashSet;
import com.amazon.coral.avail.throttlers.ThrottlerRetriever;
import com.amazon.coral.avail.throttlers.ThrottlerMemoizer;

import com.amazon.coral.avail.key.*;


/**
 * The {@code ThrottlingHandler} can be used on the server-side to throttle any
 * incoming request based on some key. The throttle keys are built based on the
 * {@code HandlerContext} by a pluggable {@code KeyGenerator}. The actual mechanism to keep
 * track of rate information is also pluggable, allowing both local and
 * distributed {@code Throttler}s to be provided. Keys that shouldn't be
 * throttled against can be specified in the list of exemption. Any request with
 * that key will not be throttled against.
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/Throttling">https://w.amazon.com/?Coral/Manual/Availability/Throttling</a>
 * <p>
 */
public class ThrottlingHandler extends UserHandler {
  private final static Log log = LogFactory.getLog(ThrottlingHandler.class);

  /**
   * The global throttling key: All throttling implementations will have a last chance to cull all incomming requests
   * on this key. This is useful for throttling decisions that need to be made regardless of the nature of the request
   * being made.
   */
  public final static String DEFAULT_KEY = "";

  private final KeyGenerator keygen;
  private final ThrottlerRetriever retriever;
  private final Set<CharSequence> exemptions;

  /** Use this to build a ThrottlingHandler.
   */
  public static final class Builder {
    // required
    KeyGenerator keygen;
    ThrottlerRetriever retriever;

    // optional
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();

    /** This throttler will be memoized, so that all Handlers
     * built by this Builder instance will use the same
     * memoized Throttler. */
    public void setThrottler(Throttler t) {
        this.retriever = new ThrottlerMemoizer(t);
    }
    /** To avoid memoization. */
    public void setThrottlerUnmemoized(Throttler t) {
        this.retriever = new ThrottlerRetriever(t);
    }
    /** The most explicit way to set the Throttler.
     * Note that ThrottlerMemoizer extends ThrottlerRetriever */
    public void setThrottlerRetriever(ThrottlerRetriever tr) {
        this.retriever = tr;
    }
    /** Handler will see on the keys that this generates.
     * No hidden globals or unexpected prefixes.
     * Required. */
    public void setKeyGenerator(KeyGenerator g) {
       this.keygen = g;
    }
    /** By default Collections.<CharSequence>emptyList()
     * Note: These exemptions apply to *all* keys from KeyGenerator. */
    public void setExemptions(Collection<CharSequence> exemptions) {
       this.exemptions = exemptions;
    }

    /** @see setThrottler */
    public Builder withThrottler(Throttler throttler) {
        setThrottler(throttler);
        return this;
    }
    /** @see setThrottlerUnmemoized */
    public Builder withThrottlerUnmemoized(Throttler throttler) {
        setThrottlerUnmemoized(throttler);
        return this;
    }
    /** @see setThrottlerRetriever */
    public Builder withThrottlerRetriever(ThrottlerRetriever t) {
        setThrottlerRetriever(t);
        return this;
    }
    /** @see setKeyGenerator */
    public Builder withKeyGenerator(KeyGenerator g) {
        setKeyGenerator(g);
        return this;
    }
    /** @see setExemptions */
    public Builder withExemptions(Collection<CharSequence> exemptions) {
        setExemptions(exemptions);
        return this;
    }


    public ThrottlingHandler build() {
      /*
       * If a user specifies a Throttler to this Builder, it is wrapped in a ThrottlerRetriever.
       * If a user specifies a com.amazon.coral.availability.ThrottlingStrategy, it is wrapped in an adapter, then wrapped in a
       * a ThrottlerRetriever. The ThrottlingHandler will perform all throttling operations against
       * the underlying Throttler, via the Retriever.
       */
      Preconditions.checkNotNull(keygen, "KeyGenerator must be set.");
      Preconditions.checkNotNull(retriever, "failed to choose ThrottlingHandler throttler");
      return new ThrottlingHandler(keygen, retriever, exemptions);
    }
  }

  /** @return ThrottlingHandler.Builder */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** This can help with migration.
   * The global key will be "".
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param usePrefix will use "ID:"
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder.buildForIdentity (preferred)*/
  public static ThrottlingHandler buildForIdentity(List<String> identities, boolean usePrefix, Throttler throttler, Collection<CharSequence> exemptions) {
    return new Builder().withThrottler(throttler).withExemptions(exemptions).withKeyGenerator(
        new KeyGeneratorBuilder().withPrefix(usePrefix?"ID:":"").withGlobalKey("").withIdentities(identities).buildForIdentity()).build();
  }
  /** This can help with migration.
   * The global key will be "".
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param usePrefix will use "ID_OP:"
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder.buildForIdentityOperation (preferred)*/
  public static ThrottlingHandler buildForIdentityOperation(List<String> identities, boolean usePrefix, Throttler throttler, Collection<CharSequence> exemptions) {
    return new Builder().withThrottler(throttler).withExemptions(exemptions).withKeyGenerator(
        new KeyGeneratorBuilder().withPrefix(usePrefix?"ID_OP:":"").withGlobalKey("").withIdentities(identities).buildForIdentityOperation()).build();
  }
  /** This can help with migration.
   * The global key will be "".
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param usePrefix will use "OP_ID:"
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder.buildForOperationIdentity (preferred)*/
  public static ThrottlingHandler buildForOperationIdentity(List<String> identities, boolean usePrefix, Throttler throttler, Collection<CharSequence> exemptions) {
    return new Builder().withThrottler(throttler).withExemptions(exemptions).withKeyGenerator(
        new KeyGeneratorBuilder().withPrefix(usePrefix?"OP_ID:":"").withGlobalKey("").withIdentities(identities).buildForOperationIdentity()).build();
  }
  /** This can help with migration.
   * The global key will be "".
   * @param usePrefix will use "OP:"
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder.buildForOperation (preferred)*/
  public static ThrottlingHandler buildForOperation(boolean usePrefix, Throttler throttler, Collection<CharSequence> exemptions) {
    return new Builder().withThrottler(throttler).withExemptions(exemptions).withKeyGenerator(
        new KeyGeneratorBuilder().withPrefix(usePrefix?"OP:":"").withGlobalKey("").buildForOperation()).build();
  }


  /** Note that we now apply exemptions to every key from the KeyGenerator, even the 'global' key. */
  ThrottlingHandler(KeyGenerator keygen, ThrottlerRetriever retriever, java.util.Collection<CharSequence> exemptions) {
    this.keygen = Preconditions.checkNotNull(keygen, "null KeyGenerator");
    this.retriever = Preconditions.checkNotNull(retriever, "null retriever");

    this.exemptions = new CharSequenceHashSet<CharSequence>();
    for(CharSequence exemption : exemptions) {
      this.exemptions.add(exemption.toString());
    }
  }

  final KeyGenerator getGenerator() {
    return keygen;
  }

  final Set<CharSequence> getExemptions() {
    return Collections.unmodifiableSet(exemptions);
  }

  @Override
  public void before(HandlerContext ctx)
    throws com.amazon.coral.availability.ThrottlingException {

    long start = System.nanoTime();
    Throttler throttler = getThrottler(ctx);

    Metrics metrics = ctx.getMetrics();

    boolean throttled = false;
    CharSequence throttledKey = null;
    try {

      Iterable<CharSequence> keys = getKeys(ctx);
      for (CharSequence key : keys) {
        if (isExempt(key))
          return;
      }

      for (CharSequence key : keys) {
        try {
          if(throttler.isThrottled(key, metrics)) {
            throttledKey = key;
            throttled = true;
          }
        } catch(Throwable failure) {
          log.error("Failure from throttler", failure);
        }

        if(throttled) {
          throw new com.amazon.coral.availability.ThrottlingException("Rate exceeded");
        }
      }
    } finally {
      emitMetrics(ctx, throttled, start, throttledKey);
    }

  }

  void emitMetrics(HandlerContext ctx, boolean throttled, long start, CharSequence throttledKey) {
    // Has to emit 0 or 1 for each option
    Metrics metrics = ctx.getMetrics();
    metrics.addCount("Throttle", (throttled ? 1.0 : 0.0), Unit.ONE);
    // Provide timing information to capture how much time is spent on throttling
    metrics.addTime("ThrottleTime", Math.max(0,System.nanoTime()-start), SI.NANO(SI.SECOND));

    if (throttled)
      metrics.addProperty("ThrottledKey", throttledKey);
  }

  protected boolean isExempt(CharSequence key) {
    return exemptions.contains(key.toString());
  }

  Throttler getThrottler(HandlerContext ctx) {
    return this.retriever.getThrottler(ctx);
  }

  private Iterable<CharSequence> getKeys(HandlerContext ctx) {
    try {
      return keygen.getKeys(ctx);
    } catch(Throwable failure) {
      log.error("Failure from KeyGenerator", failure);
    }
    return java.util.Collections.<CharSequence>emptyList();
  }
}
