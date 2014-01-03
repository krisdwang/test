package com.amazon.coral.spring.director.test.sample;

import com.amazon.coral.spring.director.live.Live;
import com.amazon.coral.tally.Tally;

public class WorkerLivePure implements Live {

    private final Tally tally;

    public WorkerLivePure(Tally tally) {
        this.tally = tally;
    }

    @Override
    public void acquire() throws Exception {
        tally.add("pure live acquire");
    }

    @Override
    public void activate() throws Exception {
        tally.add("pure live activate");
    }

    @Override
    public void deactivate() throws Exception {
        tally.add("pure live deactivate");
    }

    @Override
    public void release() throws Exception {
        tally.add("pure live release");
    }
}
