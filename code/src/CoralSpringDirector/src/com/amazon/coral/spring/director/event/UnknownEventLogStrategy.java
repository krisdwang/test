package com.amazon.coral.spring.director.event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEvent;

/**
 * A strategy that logs a WARN message when the target bean does not know how to interpret a received message.
 * 
 * @author Ovidiu Gheorghies
 */
public class UnknownEventLogStrategy implements EventStrategy {
    private static final Log log = LogFactory.getLog(UnknownEventLogStrategy.class);

    public static final UnknownEventLogStrategy INSTANCE = new UnknownEventLogStrategy();

    @Override
    public void onEvent(Object target, ApplicationEvent event) {
        String message = String.format("Unknown event %s for target %s", event.toString(), target.toString());

        log.warn(message);
    }

}
