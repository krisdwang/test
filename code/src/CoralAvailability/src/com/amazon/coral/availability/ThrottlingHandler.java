// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

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
import com.amazon.coral.service.AbstractHandler;
import com.amazon.coral.service.Constant;
import com.amazon.coral.service.Job;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.util.CharSequenceHashSet;

/**
 * The {@code ThrottlingHandler} can be used on the server-side to throttle any
 * incoming request based on some key. The throttle keys are built based on the
 * {@code Job} by a pluggable {@code KeyBuilder}. The actual mechanism to keep
 * track of rate information is also pluggable, allowing both local and
 * distributed {@code ThrottlingStrategy}s to be provided. Keys that shouldn't be
 * throttled against can be specified in the list of exemption. Any request with
 * that key will not be throttled against.
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/Throttling">https://w.amazon.com/?Coral/Manual/Availability/Throttling</a>
 * <p>
 * @Deprecated prefer com.amazon.coral.avail.handlers.ThrottlingHandler
 */
@Deprecated
public class ThrottlingHandler extends AbstractHandler {
  private final static Log log = LogFactory.getLog(ThrottlingHandler.class);

  /**
   * The global throttling key: All throttling implementations will have a last chance to cull all incomming requests
   * on this key. This is useful for throttling decisions that need to be made regardless of the nature of the request
   * being made.
   */
  public final static String DEFAULT_KEY = "";
  private final static Constant<MemoizedThrottlingStrategy> MEMOIZER = new Constant<MemoizedThrottlingStrategy>(MemoizedThrottlingStrategy.class, "Throttling Memoizer");
  private static boolean youveBeenWarned = false;

  private final KeyBuilder builder;
  /**
   * See notes on this.throttler. This strategy should no longer be directly used.
   */
  @Deprecated
  private ThrottlingStrategy strategy;
  /**
   * The interface, ThrottlingStrategy, is inadequate for current throttling requirements.
   * Since many third party throttling strategies have been built off of this interface, the Throttler
   * abstract class has been designed to extend features and compromises for compatibility.
   *
   * If a user specifies a ThrottlingStrategy to this handler, it is wrapped in an adapter and stored
   * as a Throttler. This ThrottlingHandler should perform all throttling operations against this
   * Throttler.
   */
  private final Throttler throttler;
  private final Set<CharSequence> exemptions;

  public enum Type {
    UNKNOWN, IDENTITY, OPERATION, IDENTITY_OPERATION, OPERATION_IDENTITY
  }
  public static final class Builder {
    // Exactly one of these 2 is required.
    Type cls;
    KeyBuilder keyBuilder;

    Throttler throttler; // required
    java.util.List<String> keys; // required iff type supplied

    // optional
    Collection<CharSequence> exemptions = Collections.<CharSequence>emptyList();
    boolean usePrefix = false;

    public void setType(Type cls) {
      this.cls = cls;
    }
    @Deprecated
    public void setStrategy(ThrottlingStrategy strat) {
      this.throttler = new ThrottlerAdapter(strat);
    }
    public void setThrottler(Throttler t) {
      this.throttler = t;
    }
    public void setKeys(java.util.List<String> keys) {
      this.keys = keys;
    }
    public void setKeyBuilder(KeyBuilder kb) {
      this.keyBuilder = kb;
    }
    public void setExemptions(Collection<CharSequence> exemptions) {
      this.exemptions = exemptions;
    }
    public void setUsePrefix(boolean usePrefix) {
      this.usePrefix = usePrefix;
    }

    public Builder withType(Type cls) {
      setType(cls);
      return this;
    }
    @Deprecated
    public Builder withStrategy(ThrottlingStrategy strat) {
      return withThrottler(new ThrottlerAdapter(strat));
    }
    public Builder withThrottler(Throttler t) {
      setThrottler(t);
      return this;
    }
    public Builder withKeys(java.util.List<String> keys) {
      setKeys(keys);
      return this;
    }
    public Builder withExemptions(Collection<CharSequence> exemptions) {
      setExemptions(exemptions);
      return this;
    }
    public Builder withUsePrefix(boolean usePrefix) {
      setUsePrefix(usePrefix);
      return this;
    }


    public ThrottlingHandler build() {
      Preconditions.checkState((null==cls)!=(null==keyBuilder), "Either type or keyBuilder must be set, but not both. type=" + cls + ", kb=" + keyBuilder);
      if (null != keyBuilder) {
        Preconditions.checkState((null==keys), "keys cannot be supplied with keyBuilder.");
        return new ThrottlingHandler(keyBuilder, throttler, exemptions);
      }

      switch (cls) {
        case IDENTITY:
          return IdentityThrottlingHandler.build(this);
        case OPERATION:
          return OperationThrottlingHandler.build(this);
        case IDENTITY_OPERATION:
          return IdentityOperationThrottlingHandler.build(this);
        case OPERATION_IDENTITY:
          return OperationIdentityThrottlingHandler.build(this);
        default:
          throw new RuntimeException("failed to handle enum");
      }
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
    return new IdentityThrottlingHandler(identities, (usePrefix?"ID:":""), throttler, exemptions);
  }
  /** This can help with migration.
   * The global key will be "".
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param usePrefix will use "ID_OP:"
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder.buildForIdentityOperation (preferred)*/
  public static ThrottlingHandler buildForIdentityOperation(List<String> identities, boolean usePrefix, Throttler throttler, Collection<CharSequence> exemptions) {
    return new IdentityOperationThrottlingHandler(identities, (usePrefix?"ID_OP:":""), throttler, exemptions);
  }
  /** This can help with migration.
   * The global key will be "".
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param usePrefix will use "OP_ID:"
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder.buildForOperationIdentity (preferred)*/
  public static ThrottlingHandler buildForOperationIdentity(List<String> identities, boolean usePrefix, Throttler throttler, Collection<CharSequence> exemptions) {
    return new OperationIdentityThrottlingHandler(identities, (usePrefix?"OP_ID:":""), throttler, exemptions);
  }
  /** This can help with migration.
   * The global key will be "".
   * @param usePrefix will use "OP:"
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder.buildForOperation (preferred)*/
  public static ThrottlingHandler buildForOperation(boolean usePrefix, Throttler throttler, Collection<CharSequence> exemptions) {
    return new OperationThrottlingHandler(null, (usePrefix?"OP:":""), throttler, exemptions);
  }

  @Deprecated
  public ThrottlingHandler(KeyBuilder builder, ThrottlingStrategy strategy) {
    this(builder, new ThrottlerAdapter(strategy), Collections.<CharSequence>emptyList());
    this.strategy = strategy;
  }

  @Deprecated
  public ThrottlingHandler(KeyBuilder builder, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(builder, new ThrottlerAdapter(strategy), exemptions);
    this.strategy = strategy;
  }

  ThrottlingHandler(KeyBuilder builder, Throttler throttler, Collection<CharSequence> exemptions) {
    this.builder = Preconditions.checkNotNull(builder, "null builder");
    this.throttler = Preconditions.checkNotNull(throttler, "null throttler");

    this.exemptions = new CharSequenceHashSet<CharSequence>();
    for(CharSequence exemption : exemptions) {
      this.exemptions.add(exemption.toString());
    }
  }

  @Deprecated()
  final ThrottlingStrategy getStrategy() {
    return strategy;
  }

  final KeyBuilder getBuilder() {
    return builder;
  }

  final Set<CharSequence> getExemptions() {
    return Collections.unmodifiableSet(exemptions);
  }

  @Override
  public void before(Job job)
    throws ThrottlingException {

    long start = System.nanoTime();
    Throttler throttler = getThrottler(job);

    Metrics metrics = job.getMetrics();

    boolean throttled = false;
    CharSequence throttledKey = null;
    try {

      Iterable<CharSequence> keys = getKeys(job);
      for (CharSequence key : keys) {
        if (isExempt(key))
          return;
      }
      if (!youveBeenWarned && isExempt(DEFAULT_KEY)) {
        log.warn("In CoralAvailability-2.0, the behavior will change such that the DEFAULT_KEY will be subject to exemptions. Please remove '' from the list of exemptions, or switch to CoralAvailability-2.0 now.");
        youveBeenWarned = true;
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
          throw new ThrottlingException("Rate exceeded");
        }
      }

      // If we have come this far, it means the request is not throttled
      // based on any of the Identity attributes. Try the CATCH ALL
      // throttling now.
      try {
        if(throttler.isThrottled(DEFAULT_KEY, metrics)) {
          throttledKey = DEFAULT_KEY;
          throttled = true;
        }
      } catch(Throwable failure) {
        log.error("Failure from throttler", failure);
      }

      if(throttled) {
        throw new ThrottlingException("Rate exceeded");
      }

    } finally {
      emitMetrics(job, throttled, start, throttledKey);
    }

  }

  void emitMetrics(Job job, boolean throttled, long start, CharSequence throttledKey) {
    // Has to emit 0 or 1 for each option
    Metrics metrics = job.getMetrics();
    metrics.addCount("Throttle", (throttled ? 1.0 : 0.0), Unit.ONE);
    // Provide timing information to capture how much time is spent on throttling
    metrics.addTime("ThrottleTime", Math.max(0,System.nanoTime()-start), SI.NANO(SI.SECOND));

    if (throttled)
      metrics.addProperty("ThrottledKey", throttledKey);
  }

  protected boolean isExempt(CharSequence key) {
    return exemptions.contains(key.toString());
  }

  // Produce a ThrottlingStrategy that is memoized and scoped to the Job
  @Deprecated
  ThrottlingStrategy getThrottlingStrategy(Job job) {
    ThrottlingStrategy s = job.getAttribute(MEMOIZER);
    if(s == null) {
      s = new MemoizedThrottlingStrategy(this.strategy);
      job.setAttribute(MEMOIZER, s);
    }
    if (s instanceof MemoizedThrottlingStrategy) {
      MemoizedThrottlingStrategy i = (MemoizedThrottlingStrategy)s;
      i.usedBy(this);
    }
    return s;
  }

  Throttler getThrottler(Job job) {
    Throttler t = job.getAttribute(MEMOIZER);
    if(t == null) {
      t = new MemoizedThrottlingStrategy(this.throttler);
      job.setAttribute(MEMOIZER, t);
    }
    if (t instanceof MemoizedThrottlingStrategy) {
      MemoizedThrottlingStrategy i = (MemoizedThrottlingStrategy)t;
      i.usedBy(this);
    }
    return t;
  }

  /** For now, ShadowThrottlingHandler need this. */
  @Deprecated
  Throttler getThrottlerForShadow() {
    return this.throttler;
  }

  private Iterable<CharSequence> getKeys(Job job) {
    try {
      return builder.getKeys(job);
    } catch(Throwable failure) {
      log.error("Failure from KeyBuilder", failure);
    }
    return java.util.Collections.<CharSequence>emptyList();
  }
}
