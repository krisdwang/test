// vim: et ts=8 sts=4 sw=4 tw=0
package com.amazon.coral.service;

import com.amazon.coral.annotation.BuffersRequestPayload;

public class HandlerContextAdapter extends HandlerContextImpl {
    @BuffersRequestPayload
    private static class DummyHandler extends UserHandler {}

    public HandlerContextAdapter(Job job) {
        super(new DummyHandler(), fixJob(job), true);
    }

    private static EventsJob fixJob(Job job) {
        if (!(job instanceof EventsJob)) job = new EventsJobWrapper(job);
        if (job.getAttribute(WireProtocol.WIRE_PROTOCOL_FACTORY) == null) {
            job.setAttribute(WireProtocol.WIRE_PROTOCOL_FACTORY, HttpWireProtocolFactory.INSTANCE);
        }
        return (EventsJob)job;
    }
}
