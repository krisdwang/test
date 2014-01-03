package com.amazon.coral.spring.director.event;

import org.springframework.context.ApplicationEvent;

/**
 * A strategy that throws an IllegalStateException when the target bean does not know how to interpret a received
 * message.
 * 
 * @author Ovidiu Gheorghies
 */
public class UnknownEventThrowStrategy implements EventStrategy {
    public static final UnknownEventThrowStrategy INSTANCE = new UnknownEventThrowStrategy();

    @Override
    public void onEvent(Object target, ApplicationEvent event) {
        String message = String.format("Unknown event %s for target %s", event.toString(), target.toString());
        throw new IllegalStateException(message);
    }

}
