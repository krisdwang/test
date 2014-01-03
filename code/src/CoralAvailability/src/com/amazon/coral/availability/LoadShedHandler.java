// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import java.util.Collection;
import java.util.Set;
import java.util.List;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.google.common.base.Preconditions;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.AbstractHandler;
import com.amazon.coral.service.Constant;
import com.amazon.coral.service.Job;
import com.amazon.coral.service.RequestCountHelper;
import com.amazon.coral.service.ServiceUnavailableException;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.util.CharSequenceHashSet;

/**
 * The {@code LoadShedHandler} can be used on the server-side to reject an
 * incoming request based on some key and the health of the service. The keys are
 * built based on the {@code Job} by a pluggable {@code KeyBuilder}. The actual
 * decision to throttle a key is off-loaded to a pluggable {@code ThrottlingStrategy}.
 * If a key is throttled and the service is unhealthy according to the
 * {@code HealthMonitor} then the request will be rejected.
 * Keys that shouldn't be considered for shedding can be specified in the list of
 * exemptions.
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/BrownoutProtection">https://w.amazon.com/?Coral/Manual/Availability/BrownoutProtection</a>
 * <p>
 * @Deprecated prefer com.amazon.coral.avail.handlers.LoadShedHandler
 */
@Deprecated
public class LoadShedHandler extends AbstractHandler {
  public final static String PREFIX = "droppable";

  private final static Log log = LogFactory.getLog(LoadShedHandler.class);
  private final static Constant<MemoizedThrottlingStrategy> MEMOIZER = new Constant<MemoizedThrottlingStrategy>(MemoizedThrottlingStrategy.class, "Throttling Memoizer");
  private static boolean youveBeenWarned = false;

  private final Set<CharSequence> exemptions;
  private final KeyBuilder builder;
  private final Throttler throttler;
  private final int capacity;

  public enum Type {
    IDENTITY, OPERATION, IDENTITY_OPERATION, OPERATION_IDENTITY
  }
  public static final class Builder {
    Type cls;
    ThrottlingStrategy strat;
    java.util.List<String> keys;
    Collection<CharSequence> exemptions = java.util.Collections.<CharSequence>emptyList();
    Integer capacity;

    public void setType(Type cls) {
      this.cls = cls;
    }
    public void setStrategy(ThrottlingStrategy strat) {
      this.strat = strat;
    }
    public void setKeys(java.util.List<String> keys) {
      this.keys = keys;
    }
    public void setExemptions(Collection<CharSequence> exemptions) {
      this.exemptions = exemptions;
    }
    public void setCapacity(int capacity) {
      this.capacity = capacity;
    }

    public Builder withType(Type cls) {
      setType(cls);
      return this;
    }
    public Builder withStrategy(ThrottlingStrategy strat) {
      setStrategy(strat);
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
    public Builder withCapacity(int capacity) {
      setCapacity(capacity);
      return this;
    }


    public LoadShedHandler build() {
      Preconditions.checkNotNull(cls, "failed to choose LoadShedHandler type");
      Preconditions.checkNotNull(capacity, "failed to set LoadShedHandler capacity");
      switch (cls) {
        case IDENTITY:
          return IdentityLoadShedHandler.build(this);
        case OPERATION:
          return OperationLoadShedHandler.build(this);
        case IDENTITY_OPERATION:
          return IdentityOperationLoadShedHandler.build(this);
        case OPERATION_IDENTITY:
          return OperationIdentityLoadShedHandler.build(this);
        default:
          throw new RuntimeException("failed to handle enum");
      }
    }
  }

  /** @return LoadShedHandler.Builder */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** This can help with migration.
   * The global key will be "droppable". Otherwise, keys will be prefixed with "droppable-".
   * Try KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentity() (preferred)
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param capacity max number of simultaneous requests in service chain
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder */
  public static LoadShedHandler buildForIdentity(List<String> identities, int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    return new IdentityLoadShedHandler(identities, capacity, throttler, exemptions);
  }

  /** This can help with migration.
   * The global key will be "droppable". Otherwise, keys will be prefixed with "droppable-".
   * Try KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForIdentityOperation() (preferred)
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param capacity max number of simultaneous requests in service chain
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder */
  public static LoadShedHandler buildForIdentityOperation(List<String> identities, int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    return new IdentityOperationLoadShedHandler(identities, capacity, throttler, exemptions);
  }

  /** This can help with migration.
   * The global key will be "droppable". Otherwise, keys will be prefixed with "droppable-".
   * Try KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForOperationIdentity() (preferred)
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param capacity max number of simultaneous requests in service chain
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder */
  public static LoadShedHandler buildForOperationIdentity(List<String> identities, int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    return new OperationIdentityLoadShedHandler(identities, capacity, throttler, exemptions);
  }

  /** This can help with migration.
   * The global key will be "droppable". Otherwise, keys will be prefixed with "droppable-".
   * Try KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForOperation() (preferred)
   * @param identities can be IdentityKeyBuilder.DEFAULT_ATTRIBUTES
   * @param capacity max number of simultaneous requests in service chain
   * @param throttler will be memoized
   * @param exemptions usually Collections.<CharSequence>emptyList()
   * @see KeyGeneratorBuilder */
  public static LoadShedHandler buildForOperation(int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    return new OperationLoadShedHandler(capacity, throttler, exemptions);
  }

  @Deprecated
  public LoadShedHandler(KeyBuilder builder, int capacity, ThrottlingStrategy strategy, Collection<CharSequence> exemptions) {
    this(builder, capacity, new ThrottlerAdapter(strategy), exemptions);
  }

  LoadShedHandler(KeyBuilder builder, int capacity, Throttler throttler, Collection<CharSequence> exemptions) {
    Preconditions.checkNotNull(builder, "null builder");
    Preconditions.checkNotNull(throttler, "null throttler");
    Preconditions.checkNotNull(capacity, "null capacity");
    Preconditions.checkNotNull(exemptions, "null exemptions");
    Preconditions.checkArgument(capacity >= 0);

    this.builder = new PrefixKeyBuilder(PREFIX, "-", builder);
    this.throttler = throttler;
    this.capacity = capacity;

    this.exemptions = new CharSequenceHashSet<CharSequence>();
    for(CharSequence exemption : exemptions) {
      Preconditions.checkNotNull(exemption, "null exemption");
      this.exemptions.add(exemption.toString());
    }
  }

  @Override
  public void before(Job job) throws ServiceUnavailableException {

    long start = System.nanoTime();

    Throttler throttler = getThrottler(job);

    Integer requestCount = RequestCountHelper.getCount(job);
    if (requestCount == null) {
      log.warn("The request count is missing so load on the server can't be determined. Ensure you're using the correct orchestrator.");
      return;
    }

    Metrics metrics = job.getMetrics();

    boolean shed = false;
    CharSequence shedKey = null;
    try {

      Iterable<CharSequence> keys = getKeys(job);
      for (CharSequence key : keys) {
        if (isExempt(key))
          return;
      }
      if (!youveBeenWarned && isExempt(PREFIX)) {
        log.warn("In CoralAvailability-2.0, the behavior will change such that the default key will be subject to exemptions. Please remove 'droppable' from the list of exemptions, or switch to CoralAvailability-2.0 now.");
        youveBeenWarned = true;
      }

      for (CharSequence key : keys)
        if(doThrottle(throttler, key, metrics)) {
          // the throttle call needs to be made regardless of health
          // to ensure whatever throttler is being used sees the keys
          // for each request
          if (atCapacity(job)) {
            shed = true;
            shedKey = key;

            throw new ServiceUnavailableException("The service is temporarily overloaded.");
          }
        }

      // If we have come this far, it means the request is not throttled
      // based on any of the Identity attributes. Try the CATCH ALL
      // throttling now.
      if(doThrottle(throttler, PREFIX, metrics))
        if (atCapacity(job)) {
          shed = true;
          shedKey = PREFIX;

          throw new ServiceUnavailableException("The service is temporarily overloaded.");
        }

    } finally {
      // Has to emit 0 or 1 for each option
      metrics.addCount("LoadShed", (shed ? 1.0 : 0.0), Unit.ONE);
      // Provide timing information to capture how much time is spent on throttling
      metrics.addTime("ShedTime", Math.max(0,System.nanoTime()-start), SI.NANO(SI.SECOND));

      if (shed)
        metrics.addProperty("ShedKey", shedKey);
    }

  }

  protected boolean isExempt(CharSequence key) {
    return exemptions.contains(key);
  }

  // Produce a ThrottlingStrategy that is memoized and scoped to the Job
  private Throttler getThrottler(Job job) {
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

  private Iterable<CharSequence> getKeys(Job job) {
    try {
      return builder.getKeys(job);
    } catch(Throwable failure) {
      log.error("Failure from KeyBuilder", failure);
    }
    return java.util.Collections.<CharSequence>emptyList();
  }

  private boolean doThrottle(Throttler throttler, CharSequence key, Metrics metrics)
    throws ServiceUnavailableException {

    boolean throttled = false;
    try {
      throttled = throttler.isThrottled(key, metrics);
    } catch(Throwable failure) {
      log.error("Failure from ThrottlingStrategy", failure);
    }

    if(throttled) {
      if(log.isDebugEnabled())
        log.debug("Shedding Candidate " + key);

      return true;
    }
    else
      return false;

  }

  private boolean atCapacity(Job job) {
    Integer requestCount = RequestCountHelper.getCount(job);
    return requestCount != null && requestCount.intValue() > capacity;
  }

}
