package com.amazon.coral.spring.director.live;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;

import com.amazon.coral.spring.director.event.EventStrategy;
import com.amazon.coral.spring.director.event.NamedEvent;
import com.amazon.coral.spring.director.event.UnknownEventThrowStrategy;

/**
 * Maps the NamedEvent-s "acquire", "activate", "deactivate", "release" to their namesake, abstract, method
 * correspondents.
 * 
 * @author Ovidiu Gheorghies
 */
public abstract class LiveAdapter implements ApplicationListener<NamedEvent>, Live {
    private static final Log log = LogFactory.getLog(LiveAdapter.class);

    private EventStrategy unknownEventStrategy = UnknownEventThrowStrategy.INSTANCE;

    /**
     * Translates acquire, activate, deactivate, release named events into their respective Live calls.
     */
    @Override
    public void onApplicationEvent(NamedEvent event) {
        String name = event.getPayload();

        LiveEvent liveEvent = null;

        try {
            liveEvent = LiveEvent.valueOf(name);
        } catch (IllegalArgumentException unknownEventNameException) {
            unknownEventStrategy.onEvent(this, event);
            return;
        }

        onLiveEvent(liveEvent);
    }

    private void onLiveEvent(LiveEvent liveEvent) {
        try {
            switch (liveEvent) {
            case acquire:
                acquire();
                break;
            case activate:
                activate();
                break;
            case deactivate:
                deactivate();
                break;
            case release:
                release();
                break;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setUnknownEventStrategy(EventStrategy unknownEventStrategy) {
        this.unknownEventStrategy = unknownEventStrategy;
    }
}
