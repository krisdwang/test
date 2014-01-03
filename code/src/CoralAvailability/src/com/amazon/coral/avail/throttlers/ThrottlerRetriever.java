// vim: et ts=8 sts=4 sw=4 tw=0
package com.amazon.coral.avail.throttlers;

import com.amazon.coral.google.common.base.Preconditions;

import com.amazon.coral.service.HandlerContext;
import com.amazon.coral.throttle.api.Throttler;

public class ThrottlerRetriever {
    protected Throttler throttler;

    public ThrottlerRetriever(Throttler throttler) {
        Preconditions.checkNotNull(throttler, "ThrottlerRetriever(null throttler)");
        this.throttler = throttler;
    }

    public Throttler getThrottler(HandlerContext ctx) {
        return this.throttler;
    }
}
