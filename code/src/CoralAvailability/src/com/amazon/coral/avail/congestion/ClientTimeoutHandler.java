// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail.congestion;

import static com.amazon.coral.service.ServiceConstant.X_AMZN_CLIENT_TTL;

import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.model.Model;
import com.amazon.coral.model.ModelIndex;
import com.amazon.coral.model.reflect.DetectedModelIndexFactory;
import com.amazon.coral.service.AbstractHandler;
import com.amazon.coral.service.ActivityHandler;
import com.amazon.coral.service.Context;
import com.amazon.coral.service.HttpHandler;
import com.amazon.coral.service.Job;
import com.amazon.coral.service.OperationSet;
import com.amazon.coral.service.ServiceConstant;
import com.amazon.coral.service.ServiceHelper;
import com.amazon.coral.service.ServiceUnavailableException;
import com.amazon.coral.service.TTL;

/**
 * <p>
 * The {@link ClientTimeoutHandler} is a congestion-control construct that allows
 * your service to disregard requests during brown-out conditions, if the
 * server can be reasonably certain that the client originating the request has
 * already given up waiting for an answer.
 * </p>
 * <p>
 * You can configure this handler to behave in one of two modes:
 * </p>
 * <ul>
 *  <li>
 *    Use the {@linkplain #ClientTimeoutHandler() default constructor} to create
 *    a handler that blindly drops all requests for which the originating client
 *    has timed out.
 *  </li>
 *  <li>
 *    Use the {@linkplain #ClientTimeoutHandler(Iterable) black-list constructor}
 *    to create a handler that will drop all requests for which the originating
 *    client has timed out, excluding requests for the operations provided in
 *    the black-list.
 *  </li>
 * </ul>
 * <p>
 * If you'd like to take more direct control over the treatment of client
 * timeouts, use the {@linkplain Context service context's}
 * {@linkplain Context#getClientDeadline() client deadline} to handle
 * timeout conditions manually from within your activities or interceptors.
 * </p>
 * <h3>Spring Configuration</h3>
 * <p>
 * The following demonstrates how to add a reference to the
 * {@link ClientTimeoutHandler} to your orchestrator chain:
 * </p>
 * <pre style="margin-left: 1ex; background-color: #ccc"><code>
 *  &lt;bean name="Chain" class="com.amazon.coral.service.helper.ChainHelper"&gt;
 *    ...
 *    &lt;bean class="com.amazon.coral.service.HttpHandler" /&gt;
 *    ...
 *    &lt;bean class="com.amazon.coral.avail.congestion.ClientTimeoutHandler" /&gt;
 *    ...
 *  &lt;/bean&gt;
 * </code></pre>
 * <p>
 * The job attributes necessary for this handler to work correctly are created
 * by the {@link HttpHandler}, so be sure that the <code>ClientTimeoutHandler</code>
 * entry appears in the chain <em>after</em> <code>HttpHandler</code>. In
 * general, it is a good idea to have this be one of the last handlers before
 * the {@link ActivityHandler}, in order to maximize the chances that the client
 * is still waiting by the time the activity itself begins to execute.
 * </p>
 * <p>
 * To take advantage of the operation black-listing functionality, create a
 * handler using the {@linkplain #ClientTimeoutHandler(Iterable) black-list constructor},
 * as shown here:
 * </p>
 * <pre style="margin-left: 1ex; background-color: #ccc"><code>
 *  &lt;bean name="Chain" class="com.amazon.coral.service.helper.ChainHelper"&gt;
 *    ...
 *    &lt;bean class="com.amazon.coral.service.HttpHandler" /&gt;
 *    ...
 *    &lt;bean class="com.amazon.coral.avail.congestion.ClientTimeoutHandler"&gt;
 *      &lt;constructor-arg&gt;
 *        &lt;list&gt;
 *          &lt;value&gt;<strong>WeatherService/PutLocation</strong>&lt;/value&gt;
 *          &lt;value&gt;<strong>WeatherService/Intercepted</strong>&lt;/value&gt;
 *        &lt;/list&gt;
 *      &lt;/constructor-arg&gt;
 *    &lt;/bean&gt;
 *    ...
 *  &lt;/bean&gt;
 * </code></pre>
 *
 * @author markbidd
 * @version 1.0
 */
public class ClientTimeoutHandler extends AbstractHandler {

  private final static Log log = LogFactory.getLog(ClientTimeoutHandler.class);

  private final OperationSet blacklist;

  /**
   * Initializes a client time-out handler that will immediately drop all
   * requests that it has determined to have timed out.
   */
  public ClientTimeoutHandler() {
    this.blacklist = OperationSet.newEmptySet();
  }

  /**
   * Initializes a client time-out handler that will immediately drop all
   * requests that it has determined to have timed out, excluding those for
   * operations in the provided black-list.
   * @param blacklist A list of operation names (in the format expected by
   * {@link OperationSet}; see example in the class description) whose
   * corresponding requests should not be considered droppable.
   */
  public ClientTimeoutHandler(final Iterable<String> blacklist) {
    this(blacklist, (new DetectedModelIndexFactory()).newModelIndex());
  }

  ClientTimeoutHandler(final Iterable<String> blacklist, final ModelIndex modelIndex) {
    if(blacklist == null || modelIndex == null)
      throw new IllegalArgumentException();

    this.blacklist = new OperationSet(blacklist, modelIndex);
  }

  /**
   * Checks the attributes of the provided job to determine if the client has
   * already abandoned the request. Should return <code>true</code> if and only
   * if the client's time out is known and has passed. The default
   * implementation of this method checks the
   * {@link ServiceConstant#X_AMZN_CLIENT_TTL} job attribute to access time-out
   * data transmitted by the client.
   */
  private boolean clientTimedOut(Job job) {
    TTL clientTTL = job.getAttribute(X_AMZN_CLIENT_TTL);
    return clientTTL != null && clientTTL.getTimeLeft() <= 0;
  }

  /**
   * Determines if the provided job is eligible to be dropped by the handler.
   * This method will only be called once {@link #clientTimedOut(Job)} has
   * returned <code>true</code> for the same job. The default implementation
   * either blindly assumes all requests to be droppable, or only requests for
   * operations not in a black-list, depending on the constructor used to
   * instantiate the class.
   * @return <code>true</code> if it is OK for the handler to drop the job,
   * and <code>false</code> otherwise.
   */
  private boolean isDroppable(Job job) {
    if(blacklist.isEmpty())
      return true;

    else {
      Model.Id serviceID;
      Model.Id operationID;

      Model service = ServiceHelper.getServiceModel(job);
      if(service == null)
        // Unknown service, so assume this is droppable.
        return true;
      else
        serviceID = service.getId();

      Model operation = ServiceHelper.getOperationModel(job);
      if (operation == null)
        // Unknown operation, so assume this is droppable.
        return true;
      else
        operationID = operation.getId();

      return !blacklist.contains(serviceID, operationID);
    }
  }

  /**
   * Determines whether or not the indicated job is droppable on the basis that
   * the requesting client has already abandoned it. If so, and if the request
   * is droppable (as defined by {@link #isDroppable(Job)}), the job is
   * terminated by throwing a {@link ServiceUnavailableException}. This handler
   * emits a count metric, <code>ClientTimeoutShed</code> that aggregates the
   * total number of requests dropped due to client timeouts.
   */
  @Override
  public final void before(Job job) throws ServiceUnavailableException {
    boolean requestDropped = false;

    try {
      if(clientTimedOut(job) && isDroppable(job)) {
        log.debug("ClientTimeoutHandler shedding request.");

        requestDropped = true;
        throw new ServiceUnavailableException("The service is temporarily overloaded.");
      }
    }
    finally {
      Metrics metrics = job.getMetrics();

      double count = requestDropped ? 1.0 : 0.0;
      metrics.addCount("ClientTimeoutShed", count, Unit.ONE);
    }
  }
}
