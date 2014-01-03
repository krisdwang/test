// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail;

import java.io.StringReader;

import java.util.List;

import com.amazon.coral.service.*;
import static com.amazon.coral.service.ServiceConstant.*;
import com.amazon.coral.metrics.*;
import com.amazon.coral.model.*;
import com.amazon.coral.model.reflect.*;
import com.amazon.coral.model.xml.XmlModelIndexFactory;

import com.amazon.coral.throttle.api.Throttler;

import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.ArgumentMatcher;

public class Helper {
  private final static MetricsFactory metricsFactory = new NullMetricsFactory();
  private final static ModelIndex index = new CompositeModelIndex(new DetectedModelIndexFactory().newModelIndex(), newModelIndex());
  private final static ServiceModel service = (ServiceModel)index.getModel(new Model.Id("MyService", "assembly#"));

  private Helper() {}

  public static ModelIndex getModelIndex() {
    return index;
  }

  public static OperationModel getOperation(String operationName) {
    return (OperationModel)index.getModel(new Model.Id(operationName, "assembly#"));
  }

  /* get jobs with request counts */
  public static Job newJob(Metrics metrics, TTL clientTTL, final int requestCount) {
    Job job = new JobImpl(index, new RequestImpl(metrics, null));

    job.setAttribute(SERVICE_INTERFACE_MODEL, service);
    if(clientTTL != null) job.setAttribute(X_AMZN_CLIENT_TTL, clientTTL);

    RequestCountHelper.setCount(job, new RequestCountHelper.Count() {
      @Override
      public int get() {
        return requestCount;
      }
    });

    return job;
  }

  public static Job newJob(Metrics metrics, TTL clientTTL) {
    return newJob(metrics, clientTTL, 1);
  }

  public static Job newJob(Metrics metrics, final int requestCount) {
    return newJob(metrics, null, requestCount);
  }

  public static Job newJob(int requestCount) {
    return newJob(getNullMetrics(), requestCount);
  }

  /* get jobs without request counts */
  public static Job newJob(Metrics metrics) {
    Request request = new RequestImpl(metrics);
    Job job = new JobImpl(index, request);
    job.setAttribute(SERVICE_INTERFACE_MODEL, service);
    return job;
  }

  public static Job newJob() {
    return newJob(getNullMetrics());
  }

  public static Metrics getNullMetrics() {
    return metricsFactory.newMetrics();
  }

  public static ModelIndex getNullModelIndex() {
    return new NullModelIndex();
  }

  private static ModelIndex newModelIndex() {
    String xml =
      "<?xml version=\"1.0\"?>" +
      "<definition assembly=\"assembly\" version=\"1.0\">" +
      "  <service name=\"MyService\">" +
      "    <operation target=\"OperationA\"/>" +
      "  </service>" +
      "  <operation name=\"OperationA\">" +
      "    <input target=\"Struct\"/>" +
      "    <output target=\"Struct\"/>" +
      "  </operation>" +
      "  <structure name=\"Struct\"/>" +
      "</definition>";

    return new DefaultModelIndexFactory(new XmlModelIndexFactory(new StringReader(xml))).newModelIndex();
  }

  public static ArgumentMatcher<CharSequence> matchesString(final String expected) {
    return new ArgumentMatcher<CharSequence>() {
      public boolean matches(Object got) {
        CharSequence cs = (CharSequence)got;
        boolean eq = expected.contentEquals(cs);
        //if (!eq) System.err.println("'"+expected+"' != '"+cs+"'"); // helpful for debugging
        return eq;
      }
    };
  }

  public static CharSequence matchedString(String expected) {
    return Mockito.argThat(matchesString(expected));
  }

  public static void verifyKeys(Throttler mock, String ... keys) {
    InOrder inOrder = Mockito.inOrder(mock);
    for (String key : keys) {
      inOrder.verify(mock).isThrottled(
          matchedString(key),
          Matchers.any(Metrics.class));
    }
    inOrder.verifyNoMoreInteractions();
  }

  /** @param namespace must have the form "foo#" */
  public static ServiceIdentity fakeServiceIdentity(String namespace, String serviceName, String operationName) {
    return new ServiceIdentity(
        new Model.Id(serviceName, namespace),
        new Model.Id(operationName, namespace)) {};
  }

  public static HandlerContext newHandlerContext(Job job) {
      return new HandlerContextAdapter(job);
  }
}
