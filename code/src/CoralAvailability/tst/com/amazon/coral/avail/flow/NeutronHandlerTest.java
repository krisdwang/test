package com.amazon.coral.avail.flow;

import javax.measure.unit.Unit;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.amazon.coral.metrics.BrokenMetrics;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.service.InternalFailure;
import com.amazon.coral.service.WireProtocol;
import com.amazon.coral.test.MockHandlerContext;
import com.amazon.coral.test.MockHttpWireProtocol;
import com.amazon.coral.test.NullWireProtocol;
import com.amazon.coral.validate.ValidationException;

public class NeutronHandlerTest {
  private static final NeutronHandler.Config config = new NeutronHandler.Config();

  @Before
  public void setup() {
    config.setDelay(20);
    config.setAllowedPerSecond(5);
  }

  @Test
  public void configBean() {
    NeutronHandler.Config testConfig = new NeutronHandler.Config();
    testConfig.setDelay(20);
    testConfig.setAllowedPerSecond(5);

    Assert.assertTrue(testConfig.getDelay() == 20);
    Assert.assertTrue(testConfig.getAllowedPerSecond() == 5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullConfig() {
    new NeutronHandler(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeDelay() {
    NeutronHandler.Config testConfig = new NeutronHandler.Config();
    testConfig.setDelay(-1);
    testConfig.setAllowedPerSecond(5);

    new NeutronHandler(testConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroDelay() {
    NeutronHandler.Config testConfig = new NeutronHandler.Config();
    testConfig.setDelay(0);
    testConfig.setAllowedPerSecond(5);

    new NeutronHandler(testConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeAllowedPerSeconds() {
    NeutronHandler.Config testConfig = new NeutronHandler.Config();
    testConfig.setDelay(20);
    testConfig.setAllowedPerSecond(-1);

    new NeutronHandler(testConfig);
  }

  @Test
  public void notHttpRequest() throws Throwable {
    final MockMetricsFactory metricsFactory = new MockMetricsFactory(0);
    MockHandlerContext context = new MockHandlerContext() {
      @Override
      public WireProtocol getWireProtocol() {
        return new NullWireProtocol();
      }

      @Override
      public Metrics getMetrics() {
        return metricsFactory.newMetrics();
      }
    };

    NeutronHandler handler = new MockNeutronHandler(config);
    handler.after(context);

    metricsFactory.assertTrue();
  }

  @Test
  public void validHttpResponse() throws Throwable {
    final MockMetricsFactory metricsFactory = new MockMetricsFactory(0);
    HandlerContext context = new MockHandlerContext() {
      @Override
      public WireProtocol getWireProtocol() {
        MockHttpWireProtocol p = new MockHttpWireProtocol() {
        };
        return p;
      }

      @Override
      public boolean hasRequestFailed() {
        return false;
      }

      @Override
      public Metrics getMetrics() {
        return metricsFactory.newMetrics();
      }
    };

    NeutronHandler handler = new MockNeutronHandler(config);
    handler.after(context);

    metricsFactory.assertTrue();
  }

  @Test
  public void failWithNoHttpResponseCode() throws Throwable {
    final MockMetricsFactory metricsFactory = new MockMetricsFactory(0);
    HandlerContext context = new MockHandlerContext() {
      @Override
      public WireProtocol getWireProtocol() {
        MockHttpWireProtocol p = new MockHttpWireProtocol() {
        };
        return p;
      }

      @Override
      public boolean hasRequestFailed() {
        return true;
      }

      @Override
      public Throwable getRequestFailure() {
        return new RuntimeException();
      }

      @Override
      public double getCurrentRequestLifetimeSeconds() {
        return 0.0;
      }

      @Override
      public Metrics getMetrics() {
        return metricsFactory.newMetrics();
      }
    };

    config.setAllowedPerSecond(1);
    NeutronHandler handler = new MockNeutronHandler(config);
    handler.after(context);
    metricsFactory.setExpectedDelay(20);
    handler.after(context);

    metricsFactory.assertTrue();
  }

  @Test
  public void validationFailure() throws Throwable {
    final MockMetricsFactory metricsFactory = new MockMetricsFactory(0);
    HandlerContext context = new MockHandlerContext() {
      @Override
      public WireProtocol getWireProtocol() {
        MockHttpWireProtocol p = new MockHttpWireProtocol() {
        };
        return p;
      }

      @Override
      public boolean hasRequestFailed() {
        return true;
      }

      @Override
      public Throwable getRequestFailure() {
        return new ValidationException();
      }

      @Override
      public Metrics getMetrics() {
        return metricsFactory.newMetrics();
      }
    };

    config.setAllowedPerSecond(1);
    NeutronHandler handler = new MockNeutronHandler(config);
    handler.after(context);
    handler.after(context);

    metricsFactory.assertTrue();
  }

  @Test
  public void internalFailure() throws Throwable {
    final MockMetricsFactory metricsFactory = new MockMetricsFactory(0);
    HandlerContext context = new MockHandlerContext() {
      @Override
      public WireProtocol getWireProtocol() {
        MockHttpWireProtocol p = new MockHttpWireProtocol() {
        };
        return p;
      }

      @Override
      public boolean hasRequestFailed() {
        return true;
      }

      @Override
      public Throwable getRequestFailure() {
        return new InternalFailure();
      }

      @Override
      public double getCurrentRequestLifetimeSeconds() {
        return 0.0;
      }

      @Override
      public Metrics getMetrics() {
        return metricsFactory.newMetrics();
      }
    };

    config.setAllowedPerSecond(1);
    NeutronHandler handler = new MockNeutronHandler(config);
    handler.after(context);
    metricsFactory.setExpectedDelay(20);
    handler.after(context);

    metricsFactory.assertTrue();
  }

  @Test
  public void internalFailureWithPartialLifetime() throws Throwable {
    final MockMetricsFactory metricsFactory = new MockMetricsFactory(0);
    HandlerContext context = new MockHandlerContext() {
      @Override
      public WireProtocol getWireProtocol() {
        MockHttpWireProtocol p = new MockHttpWireProtocol() {
        };
        return p;
      }

      @Override
      public boolean hasRequestFailed() {
        return true;
      }

      @Override
      public Throwable getRequestFailure() {
        return new InternalFailure();
      }

      @Override
      public double getCurrentRequestLifetimeSeconds() {
        return 0.005; // 5 milliseconds
      }

      @Override
      public Metrics getMetrics() {
        return metricsFactory.newMetrics();
      }
    };

    config.setAllowedPerSecond(1);
    NeutronHandler handler = new MockNeutronHandler(config);
    handler.after(context);
    metricsFactory.setExpectedDelay(15);
    handler.after(context);

    metricsFactory.assertTrue();
  }

  @Test
  public void internalFailureExceedingLifetime() throws Throwable {
    final MockMetricsFactory metricsFactory = new MockMetricsFactory(0);
    HandlerContext context = new MockHandlerContext() {
      @Override
      public WireProtocol getWireProtocol() {
        MockHttpWireProtocol p = new MockHttpWireProtocol() {
        };
        return p;
      }

      @Override
      public boolean hasRequestFailed() {
        return true;
      }

      @Override
      public Throwable getRequestFailure() {
        return new InternalFailure();
      }

      @Override
      public double getCurrentRequestLifetimeSeconds() {
        return 0.040; // 40 milliseconds
      }

      @Override
      public Metrics getMetrics() {
        return metricsFactory.newMetrics();
      }
    };

    config.setAllowedPerSecond(1);
    NeutronHandler handler = new MockNeutronHandler(config);
    handler.after(context);
    handler.after(context);

    metricsFactory.assertTrue();
  }

  private class MockMetricsFactory {
    final boolean[] gotMetrics = new boolean[1];
    final double[] expectedValue = new double[1];

    public MockMetricsFactory(int expectedValue) {
      this.expectedValue[0] = expectedValue;
    }

    public Metrics newMetrics() {
      return new BrokenMetrics() {
        @Override
        public void addTime(String name, double value, Unit<?> unit, int repeat) {
          Assert.assertTrue(name.startsWith("NeutronDelay"));
          Assert.assertEquals(expectedValue[0], value);
          gotMetrics[0] = true;
        }
      };
    }

    public void setExpectedDelay(int expectedDelay) {
      this.expectedValue[0] = expectedDelay;
    }

    public void assertTrue() {
      Assert.assertTrue(gotMetrics[0]);
    }
  }

  // Mocked out time for test
  private static class MockNeutronHandler extends NeutronHandler {
    private long now = 0;

    public MockNeutronHandler(Config config) {
      super(config);
    }

    public void setTime(long now) {
      this.now = now;
    }

    @Override
    protected long currentTimeMillis() {
      return now;
    }

  }
}
