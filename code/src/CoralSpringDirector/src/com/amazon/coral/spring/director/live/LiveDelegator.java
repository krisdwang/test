package com.amazon.coral.spring.director.live;

/**
 * A LiveAdapter that delegates to a Live implementation.
 * 
 * @author Ovidiu Gheorghies
 */
public class LiveDelegator extends LiveAdapter {

    private Live delegate;

    public void setDelegate(Live delegate) {
        this.delegate = delegate;
    }

    @Override
    public void acquire() throws Exception {
        delegate.acquire();
    }

    @Override
    public void activate() throws Exception {
        delegate.activate();
    }

    @Override
    public void deactivate() throws Exception {
        delegate.deactivate();
    }

    @Override
    public void release() throws Exception {
        delegate.release();
    }
}
