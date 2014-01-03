// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.throttlers;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.availability.DThrottleClient;
import com.amazon.coral.availability.DThrottleClientFactory;
import com.amazon.coral.availability.DThrottlePooledClient;
import com.amazon.coral.availability.DThrottleSharedClient;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.throttle.api.Throttler;

/**
 * This employs a simple rate-based algorithm.
 * This allows any service instance to have some basic protection against
 * an unexpected burst of requets.
 */
public class DistributedThrottler extends Throttler {
  private static final Log log = LogFactory.getLog(DistributedThrottler.class);
  private final static String DEFAULT_KEY = ""; // same as ThrottlingHandler.DEFAULT_KEY

  private final DThrottleClient client;

    /**
     * Use this to build a DistributedThrottler.
     */
    public static final class Builder {
        private String host = "localhost"; // Default host is localhost
        private int port = 6969; // Default port is 6969
        private int poolSize = 10; // Default pool size
        private Integer connectTimeout = null;
        private Integer soTimeout = null;
        private Boolean blackout = null;

        public void setUri(String uri) {
            if (uri == null) {
                throw new IllegalArgumentException("null uri specified.");
            }
            URI u;
            try {
                u = getURI(uri);
                host = u.getHost();
                port = u.getPort();
            } catch (URISyntaxException e) {
                log.error("Incorrect URI Syntax found: " + uri);
                throw new IllegalArgumentException(e);
            }
        }

        public void setHost(String host) {
            if (host == null) {
                throw new IllegalArgumentException("null host specified.");
            }
            this.host = host;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public void setPoolSize(int size) {
            if (size < 1) {
                throw new IllegalArgumentException("Pool size too small: " + size);
            }
            this.poolSize = size;
        }

        public void setConnectTimeout(int connectTimeout) {
            if (connectTimeout < 0) {
                throw new IllegalArgumentException("Invalid connection Timeout value, less than zero");
            }
            this.connectTimeout = connectTimeout;
        }

        /**
         */
        public void setSoTimeout(int soTimeout) {
            if (soTimeout <= 0) {
                throw new IllegalArgumentException("Invalid soTimeout value, not positive");
            }
            this.soTimeout = soTimeout;
        }

        /**
         */
        public void setBlackout(boolean blackout) {
            this.blackout = blackout;
        }

        public Builder withUri(String uri) {
            setUri(uri);
            return this;
        }

        public Builder withHost(String host) {
            setHost(host);
            return this;
        }

        public Builder withPort(int port) {
            setPort(port);
            return this;
        }

        public Builder withPoolSize(int size) {
            setPoolSize(size);
            return this;
        }

        public Builder withConnectTimeout(int connectTimeout) {
            setConnectTimeout(connectTimeout);
            return this;
        }

        public Builder withSoTimeout(int soTimeout) {
            setSoTimeout(soTimeout);
            return this;
        }

        public Builder withBlackout(boolean blackout) {
            setBlackout(blackout);
            return this;
        }

        public DistributedThrottler build() {
            DThrottleClientFactory factory = new DThrottleClientFactory(host, port);
            if (connectTimeout != null) {
                factory.setConnectTimeout(connectTimeout);
            }
            if (soTimeout != null) {
                factory.setSoTimeout(soTimeout);
            }
            if (blackout != null) {
                factory.setBlackout(blackout);
            }
            return new DistributedThrottler(new DThrottlePooledClient(factory, poolSize));
        }
    }

  /**
   */
  public DistributedThrottler() {
    this("localhost:6969");
  }

  /**
   */
  public DistributedThrottler(String uri) {
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

  public DistributedThrottler(DThrottleSharedClient client) {
    if (client == null)
      throw new IllegalArgumentException();
    this.client = client;
    // Log something useful for people trying to understand their service
    if(log.isInfoEnabled())
      log.info("Using DThrottle: " + client);
  }

  public DistributedThrottler(DThrottlePooledClient client) {
    if (client == null) {
      throw new IllegalArgumentException();
    }
    this.client = client;
    // Log something useful for people trying to understand their service
    if (log.isInfoEnabled()) {
      log.info("Using DThrottle: " + client);
    }
  }

  /**
   */
  @Override
  public boolean isThrottled(CharSequence key, Metrics metrics) {
    // Temporary hack to disable empty-string throttling with DThrottle.
    // Throttling the empty-string essentially caps the overall request
    // rate to the service to whatever DThrottle's default rate is.
    if (DEFAULT_KEY.contentEquals(key))
      return false;
    try {
      return client.query(key);
    } catch(Throwable failure) {
      log.error("Failed to query DThrottle", failure);
    }
    return false;
  }

    static URI getURI(String uri) throws URISyntaxException {
    boolean containsScheme = uri.contains("://");
    if(containsScheme == false)
      uri = "tcp://" + uri;

        return new URI(uri);
  }

}
