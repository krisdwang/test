// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.congestion;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.measure.unit.Unit;

import junit.framework.Assert;

import org.junit.Test;

import com.amazon.coral.metrics.Group;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.model.Model;
import com.amazon.coral.model.ModelIndex;
import com.amazon.coral.model.OperationModel;
import com.amazon.coral.model.ServiceModel;
import com.amazon.coral.model.reflect.DetectedModelIndexFactory;
import com.amazon.coral.service.Constant;
import com.amazon.coral.service.Job;
import com.amazon.coral.service.Reply;
import com.amazon.coral.service.Request;
import com.amazon.coral.service.ServiceConstant;
import com.amazon.coral.service.ServiceUnavailableException;
import com.amazon.coral.service.TTL;

public class ClientTimeoutHandlerTest {

  @Test
  public void testConstructor() {
    new ClientTimeoutHandler();
  }

  @Test(expected = ServiceUnavailableException.class)
  public void testTimedOutJob() {
    ClientTimeoutHandler h = new ClientTimeoutHandler();

    TTL ttl = new TTL(0);
    Job job = new TestJob(ttl);

    // Guarantee that the job is expired.
    waitUntilAfter(ttl);

    // This should throw an exception.
    h.before(job);
  }

  @Test
  public void testUnexpiredJob() {
    ClientTimeoutHandler h = new ClientTimeoutHandler();

    ExpectedMetrics m = ExpectedMetrics.createForUndroppedTestCase();

    TTL ttl = new TTL(314159);
    Job job = new TestJob(ttl, m);

    h.before(job);
    m.assertComplete();
  }

  @Test
  public void testUnknownExpirationJob() {
    ClientTimeoutHandler h = new ClientTimeoutHandler();
    ExpectedMetrics m = ExpectedMetrics.createForUndroppedTestCase();

    h.before(new TestJob(null, m));
    m.assertComplete();
  }

  @Test
  public void testDroppedMetrics() {
    ClientTimeoutHandler h = new ClientTimeoutHandler();
    ExpectedMetrics m = ExpectedMetrics.createForDroppedTestCase();

    // Make sure the request expires.
    TTL ttl = new TTL(0);
    waitUntilAfter(ttl);

    // Run the job and make sure we get an exception; verify that the
    // expected metrics are recorded.
    try {
      h.before(new TestJob(ttl, m));
      Assert.fail("Did not get exception");
    }
    catch(ServiceUnavailableException e) {
      m.assertComplete();
    }
  }

  @Test
  public void testUndroppedMetrics() {
    ClientTimeoutHandler h = new ClientTimeoutHandler();
    ExpectedMetrics m = ExpectedMetrics.createForUndroppedTestCase();

    // Run the job and verify that the expected metrics are recorded.
    h.before(new TestJob(new TTL(314159), m));
    m.assertComplete();
  }

  @Test(expected = ServiceUnavailableException.class)
  public void testBlacklistExpiredJobNotInList() {
    ModelIndex index = (new ClientTimeoutTestModelIndexFactory()).newModelIndex();

    ClientTimeoutHandler h =
      new ClientTimeoutHandler(
        Arrays.asList( "fooService/mierda", "fooService/schijt" ),
        index
      );

    TTL ttl = new TTL(0);
    ExpectedMetrics m = ExpectedMetrics.createForDroppedTestCase();

    TestJob job = new TestJob(index, "fooService", "scheisse", ttl, m);

    // Make sure that the job has time to expire.
    ClientTimeoutHandlerTest.waitUntilAfter(ttl);

    // Call the job and make sure it did the right thing in the metrics.
    try {
      h.before(job);
    }
    catch(ServiceUnavailableException e) {
      m.assertComplete();
      throw e;
    }
  }

  @Test
  public void testBlacklistExpiredJobInList() {
    ModelIndex index = (new ClientTimeoutTestModelIndexFactory()).newModelIndex();

    ClientTimeoutHandler h =
      new ClientTimeoutHandler(
        Arrays.asList( "fooService/mierda", "fooService/schijt" ),
        index
      );

    TTL ttl = new TTL(0);
    ExpectedMetrics m = ExpectedMetrics.createForUndroppedTestCase();

    TestJob job = new TestJob(index, "fooService", "mierda", ttl, m);

    // Make sure that the job has time to expire.
    ClientTimeoutHandlerTest.waitUntilAfter(ttl);

    // Call the job and make sure it did the right thing in the metrics.
    h.before(job);
    m.assertComplete();
  }

  /**
   * Waits until the current system time is strictly greater than the provided
   * deadline.
   */
  public static void waitUntilAfter(TTL ttl) {
    while(System.currentTimeMillis() < ttl.getDeadline())
      Thread.yield();
  }

  /**
   * A metrics type that only supports the APIs used by the client timeout
   * handler, and compares recorded values to expected values.
   */
  protected static class ExpectedMetrics extends Metrics {

    @SuppressWarnings("serial")
    public static ExpectedMetrics createForDroppedTestCase() {
      return new ExpectedMetrics(
        new HashMap<String, Data>() {{
          put(
            "ClientTimeoutShed",
            new ExpectedMetrics.Data(1.0, Unit.ONE, 1)
          );
        }},
        new HashMap<String, Data>(),
        new HashMap<String, CharSequence>()
      );
    }

    @SuppressWarnings("serial")
    public static ExpectedMetrics createForUndroppedTestCase() {
      return new ExpectedMetrics(
          new HashMap<String, Data>() {{
            put(
              "ClientTimeoutShed",
              new ExpectedMetrics.Data(0.0, Unit.ONE, 1)
            );
          }},
          new HashMap<String, Data>(),
          new HashMap<String, CharSequence>()
        );
    }

    private Map<String, Data> counts;
    private Map<String, Data> times;
    private Map<String, CharSequence> props;

    public ExpectedMetrics(
      Map<String, Data> counts,
      Map<String, Data> times,
      Map<String, CharSequence> props
    ) {
      this.counts = counts;
      this.times = times;
      this.props = props;
    }

    @Override
    public void addCount(String name, double value, Unit<?> unit, int repeat) {
      Assert.assertTrue(counts.containsKey(name));
      Assert.assertEquals(counts.get(name), new Data(value, unit, repeat));

      counts.remove(name);
    }

    @Override
    public void addProperty(String name, CharSequence value) {
      Assert.assertTrue(props.containsKey(name));
      Assert.assertEquals(props.get(name), value);

      props.remove(name);
    }

    @Override
    public void addTime(String name, double value, Unit<?> unit, int repeat) {
      Assert.assertTrue(times.containsKey(name));
      Assert.assertEquals(times.get(name), new Data(value, unit, repeat));

      times.remove(name);
    }

    public void assertComplete() {
      if(!counts.isEmpty())
        Assert.fail(
          "Expected metrics not present: " + getFirst(counts)
        );

      if(!times.isEmpty())
        Assert.fail(
          "Expected metrics not present: " + getFirst(times)
        );

      if(!props.isEmpty())
        Assert.fail(
          "Expected metrics not present: " + getFirst(props)
        );
    }

    private <K,V> String getFirst(Map<K,V> map) {
      for(K k : map.keySet())
        return k + ": " + map.get(k);

      return "";
    }

    @Override
    public void addDate(String name, double value) { throw new UnsupportedOperationException(); }
    @Override
    public void addLevel(String name, double value, Unit<?> unit, int repeat) { throw new UnsupportedOperationException(); }
    @Override
    public void close() { throw new UnsupportedOperationException(); }
    @Override
    public MetricsFactory getMetricsFactory() { throw new UnsupportedOperationException(); }
    @Override
    public Metrics newMetrics(Group group) { throw new UnsupportedOperationException(); }

    public static class Data {
      public double value;
      public Unit<?> unit;
      public int repeat;

      public Data(double value, Unit<?> unit, int repeat) {
        this.value = value;
        this.unit = unit;
        this.repeat = repeat;
      }

      @Override
      public boolean equals(Object obj) {
        if(obj == null)
          return false;
        else if(!(obj instanceof Data))
          return false;
        else if(obj == this)
          return true;
        else {
          Data other = (Data)obj;
          return
            value == other.value &&
            unit.equals(other.unit) &&
            repeat == other.repeat;
        }
      }

      @Override
      public int hashCode() {
        return
          new Double(value).hashCode() +
          unit.hashCode() +
          repeat;
      }

      @Override
      public String toString() {
        return "{" + value + ", " + unit + ", " + repeat + "}";
      }
    }
  }

  /**
   * A mock job that only supports the {@link Job} APIs used specifically by
   * the client timeout handler.
   */
  public static class TestJob implements Job {

    private TTL ttl;
    private Metrics metrics;
    private ModelIndex index;
    private ServiceModel service;
    private OperationModel operation;

    public TestJob(TTL ttl) {
      this(ttl, new NullMetricsFactory().newMetrics());
    }

    public TestJob(TTL ttl, Metrics metrics) {
      this.ttl = ttl;
      this.metrics = metrics;
    }

    public TestJob(ModelIndex index, String service, String operation, TTL ttl) {
      this(ttl);
      this.index = index;
      this.service = (ServiceModel)index.getModel(new Model.Id(service, "clientTimeoutHandler#"));
      this.operation = (OperationModel)index.getModel(new Model.Id(operation, "clientTimeoutHandler#"));
    }

    public TestJob(ModelIndex index, String service, String operation, TTL ttl, Metrics metrics) {
      this(ttl, metrics);
      this.index = index;
      this.service = (ServiceModel)index.getModel(new Model.Id(service, "clientTimeoutHandler#"));
      this.operation = (OperationModel)index.getModel(new Model.Id(operation, "clientTimeoutHandler#"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getAttribute(Constant<?> key) {
      if(key == ServiceConstant.SERVICE_INTERFACE_MODEL)
        return (T)service;
      else if(key == ServiceConstant.SERVICE_OPERATION_MODEL)
        return (T)operation;
      else if(key == ServiceConstant.X_AMZN_CLIENT_TTL)
        return (T)ttl;
      else
        throw new UnsupportedOperationException(
          "Can't return attribute " + key + "."
        );
    }

    @Override
    public Metrics getMetrics() { return metrics; }

    @Override
    public ModelIndex getModelIndex() { return index; }

    @Override
    public Iterable<Constant<?>> getAttributeConstants() { throw new UnsupportedOperationException(); }
    @Override
    public long getCreation() { throw new UnsupportedOperationException(); }
    @Override
    public Throwable getFailure() { throw new UnsupportedOperationException(); }
    @Override
    public CharSequence getId() { throw new UnsupportedOperationException(); }
    @Override
    public Reply getReply() { throw new UnsupportedOperationException(); }
    @Override
    public Request getRequest() { throw new UnsupportedOperationException(); }
    @Override
    public boolean isDone() { throw new UnsupportedOperationException(); }
    @Override
    public void setAttribute(Constant<?> key, Object value) { throw new UnsupportedOperationException(); }
    @Override
    public void setDone() { throw new UnsupportedOperationException(); }
    @Override
    public void setFailure(Throwable failure) { throw new UnsupportedOperationException(); }
  }
}
