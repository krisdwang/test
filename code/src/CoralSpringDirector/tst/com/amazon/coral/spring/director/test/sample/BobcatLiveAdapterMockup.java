package com.amazon.coral.spring.director.test.sample;

import com.amazon.coral.spring.director.live.LiveAdapter;

public class BobcatLiveAdapterMockup extends LiveAdapter {
    BobcatMockup bobcat;

    public void setTarget(BobcatMockup bobcat) {
        this.bobcat = bobcat;
    }

    @Override
    public void acquire() throws Exception {
        bobcat.mockAcquire();
    }

    @Override
    public void activate() throws Exception {
        bobcat.mockActivate();
    }

    @Override
    public void deactivate() throws Exception {
        bobcat.mockDeactivate();
    }

    @Override
    public void release() throws Exception {
        bobcat.mockRelease();
    }
}
