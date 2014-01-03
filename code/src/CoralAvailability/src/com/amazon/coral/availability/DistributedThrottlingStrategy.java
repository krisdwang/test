package com.amazon.coral.availability;

import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.availability.DThrottleClient;
import com.amazon.coral.availability.DThrottleSharedClient;

/**
 * A {@code DistributedThrottlingStrategy} employs a simple rate-based algorithm.
 * This allows any service instance to have some basic protection against
 * an unexpected burst of requets.
 *
 * @author Eric Crahen &lt;crahen@amazon.com&gt;
 * @version 1.0
 * @Deprecated Prefer DistributedThrottler
 */
@Deprecated
public class DistributedThrottlingStrategy implements ThrottlingStrategy {

  private final DThrottleClient client;
  private final Log log = LogFactory.getLog(DistributedThrottlingStrategy.class);

  /**
   */
  public DistributedThrottlingStrategy() {
    this("localhost:6969");
  }

  /**
   */
  public DistributedThrottlingStrategy(String uri) {
    if(uri == null)
      throw new IllegalArgumentException();
    try {
      URI u = getURI(uri);
      this.client = new DThrottleSharedClient(u.getHost(), u.getPort());
    } catch(Throwable t) {
      throw new IllegalArgumentException(t);
    }
    // Log something useful for people trying to understand their service
    if(log.isInfoEnabled())
      log.info("Using DThrottle: " + client);
  }

  public DistributedThrottlingStrategy(DThrottleSharedClient client) {
    if (client == null)
      throw new IllegalArgumentException();
    this.client = client;
    // Log something useful for people trying to understand their service
    if(log.isInfoEnabled())
      log.info("Using DThrottle: " + client);
  }

  /**
   */
  @Override
  public boolean isThrottled(CharSequence key) {
    // Temporary hack to disable empty-string throttling with DThrottle.
    // Throttling the empty-string essentially caps the overall request
    // rate to the service to whatever DThrottle's default rate is.
    if (ThrottlingHandler.DEFAULT_KEY.contentEquals(key))
      return false;
    try {
      return client.query(key);
    } catch(Throwable failure) {
      log.error("Failed to query DThrottle", failure);
    }
    return false;
  }

  static URI getURI(String uri) {
    boolean containsScheme = uri.contains("://");
    if(containsScheme == false)
      uri = "tcp://" + uri;

    return URI.create(uri);
  }

}
// Last modified by RcsDollarAuthorDollar on RcsDollarDateDollar
