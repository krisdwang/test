// vim: et ts=8 sts=4 sw=4 tw=0
package com.amazon.coral.service;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.service.UserHandler;
import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.service.HandlerContextImpl;
import com.amazon.coral.service.EventsJob;
import com.amazon.coral.service.EventsJobWrapper;
import com.amazon.coral.service.JobImpl;
import com.amazon.coral.service.RequestImpl;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.model.NullModelIndex;

public class MyHandlerContext {
    public static HandlerContext create(boolean isBefore) {
        EventsJob job = fixJob(new JobImpl(new NullModelIndex(), new RequestImpl(new NullMetricsFactory().newMetrics())));
        job.setAttribute(UserHandler.USER_HANDLER_BEFORE, isBefore);
        return new HandlerContextImpl(new UserHandler(){}, job, false);
    }
    private static EventsJob fixJob(Job job) {
        if (!(job instanceof EventsJob)) job = new EventsJobWrapper(job);
        if (job.getAttribute(WireProtocol.WIRE_PROTOCOL_FACTORY) == null) {
            job.setAttribute(WireProtocol.WIRE_PROTOCOL_FACTORY, HttpWireProtocolFactory.INSTANCE);
        }
        return (EventsJob)job;
    }
}
