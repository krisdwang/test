// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.congestion;

import java.util.Collection;
import java.util.Set;
import java.util.List;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.google.common.base.Preconditions;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.UserHandler;
import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.service.RequestCountHelper;
import com.amazon.coral.service.ServiceUnavailableException;
import com.amazon.coral.throttle.api.Throttler;
import com.amazon.coral.util.CharSequenceHashSet;
import com.amazon.coral.avail.throttlers.ThrottlerRetriever;
import com.amazon.coral.avail.throttlers.ThrottlerMemoizer;

import com.amazon.coral.avail.key.*;


/**
 * The {@code LoadShedHandler} can be used on the server-side to reject an
 * incoming request based on some key and the health of the service. The keys are
 * generated based on the {@code HandlerContext} by a pluggable {@code KeyGenerator}. The actual
 * decision to throttle a key is off-loaded to a pluggable {@code Throttler}.
 * If a key is throttled and the service is unhealthy according to the
 * {@code HealthMonitor} then the request will be rejected.
 * Keys that shouldn't be considered for shedding can be specified in the list of
 * exemptions.
 * <p>
 * <a href="https://w.amazon.com/?Coral/Manual/Availability/BrownoutProtection">https://w.amazon.com/?Coral/Manual/Availability/BrownoutProtection</a>
 * <p>
 */
public class LoadShedHandler extends UserHandler {
  public final static String PREFIX = "droppable";

  private final static Log log = LogFactory.getLog(LoadShedHandler.class);
  private static boolean youveBeenWarned = false;

  private final Set<CharSequence> exemptions;
  private final KeyGenerator keygen;
  private final ThrottlerRetriever retriever;
  private final int capacity;

  public static final class Builder {
    // required
    ThrottlerRetriever retriever;
    KeyGenerator keygen;
    Integer capacity;

    // optional
    Collection<CharSequence> exemptions = java.util.Collections.<CharSequence>emptyList();

    /**
     * Called by newBuilder() static factory method. */
    Builder() {
    }

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
    /** Maximum number of concurrent Requests in service Chain. Required. */
    public void setCapacity(int capacity) {
      this.capacity = capacity;
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
    /** @see setCapacity */
    public Builder withCapacity(int capacity) {
      setCapacity(capacity);
      return this;
    }


    public LoadShedHandler build() {
      Preconditions.checkNotNull(keygen, "failed to set KeyGenerator");
      Preconditions.checkNotNull(retriever, "failed to set LoadShedHandler throttler");
      Preconditions.checkNotNull(capacity, "failed to set LoadShedHandler capacity");

      return new LoadShedHandler(keygen, capacity, retriever, exemptions);
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
    return new Builder().withCapacity(capacity).withThrottler(throttler).withExemptions(exemptions).withKeyGenerator(
        KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentities(identities).buildForIdentity()).build();
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
    return new Builder().withCapacity(capacity).withThrottler(throttler).withExemptions(exemptions).withKeyGenerator(
        KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentities(identities).buildForIdentityOperation()).build();
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
    return new Builder().withCapacity(capacity).withThrottler(throttler).withExemptions(exemptions).withKeyGenerator(
        KeyGeneratorBuilder.newBuilderForLoadShedHandler().withIdentities(identities).buildForOperationIdentity()).build();
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
    return new Builder().withCapacity(capacity).withThrottler(throttler).withExemptions(exemptions).withKeyGenerator(
        KeyGeneratorBuilder.newBuilderForLoadShedHandler().buildForOperation()).build();
  }

  LoadShedHandler(KeyGenerator keygen, int capacity, ThrottlerRetriever retriever, Collection<CharSequence> exemptions) {
    Preconditions.checkNotNull(keygen, "null KeyGenerator");
    Preconditions.checkArgument(capacity >= 0, "negative capacity");
    Preconditions.checkNotNull(exemptions, "null exemptions");
    this.retriever = Preconditions.checkNotNull(retriever, "null Retriever");
    this.keygen = keygen;
    this.capacity = capacity;
    this.exemptions = new CharSequenceHashSet<CharSequence>();
    for(CharSequence exemption : exemptions) {
      Preconditions.checkNotNull(exemption, "null exemption");
      this.exemptions.add(exemption.toString());
    }
  }

  /** Unlike the ThrottlingHandler, the LoadShedHandler uses its PREFIX
   * as its default key.
   */
  @Override
  public void before(HandlerContext ctx) throws ServiceUnavailableException {

    long start = System.nanoTime();

    Throttler throttler = getThrottler(ctx);

    Integer requestCount = ctx.getEstimatedInflightRequests();
    if (requestCount == null) {
      log.warn("The request count is missing so load on the server can't be determined. Ensure you're using the correct orchestrator."); // TODO: Log only once? Make it an error?
      return;
    }

    Metrics metrics = ctx.getMetrics();

    boolean shed = false;
    CharSequence shedKey = null;
    try {

      Iterable<CharSequence> keys = getKeys(ctx);
      for (CharSequence key : keys) {
        if (isExempt(key))
          return;
      }
      if (!youveBeenWarned && isExempt(PREFIX)) {
        log.warn("In CoralAvailability-2.0, the behavior will change such that the default key will be subject to exemptions. Please remove 'droppable' from the list of exemptions, or switch to CoralAvailability-2.0 now.");
        youveBeenWarned = true;
      }

      for (CharSequence key : keys) {
        if(doThrottle(throttler, key, metrics)) {
          // the throttle call needs to be made regardless of health
          // to ensure whatever throttler is being used sees the keys
          // for each request
          if (atCapacity(ctx)) {
            shed = true;
            shedKey = key;

            throw new ServiceUnavailableException("The service is temporarily overloaded.");
          }
        }
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

  // Produce a Throttler that is memoized and scoped to the Job
  private Throttler getThrottler(HandlerContext ctx) {
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

  private boolean doThrottle(Throttler throttler, CharSequence key, Metrics metrics)
    throws ServiceUnavailableException {

    boolean throttled = false;
    try {
      throttled = throttler.isThrottled(key, metrics);
    } catch(Throwable failure) {
      log.error("Failure from Throttler strategy", failure);
    }

    if(throttled) {
      if(log.isDebugEnabled())
        log.debug("Shedding Candidate " + key);

      return true;
    }
    else
      return false;

  }

  private boolean atCapacity(HandlerContext ctx) {
    Integer requestCount = ctx.getEstimatedInflightRequests(); // null check is earlier
    return requestCount.intValue() > capacity;
  }

}
