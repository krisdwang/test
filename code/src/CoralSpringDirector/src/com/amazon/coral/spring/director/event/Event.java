package com.amazon.coral.spring.director.event;

import org.springframework.context.ApplicationEvent;

/**
 * A Spring event containing an object payload of type T.
 * 
 * @author Ovidiu Gheorghies
 * @param <T>
 *            The type of the event payload.
 */
public class Event<T> extends ApplicationEvent {
    private static final long serialVersionUID = 413056819211099068L;

    protected T payload;

    protected Event(Object source) {
        super(source);
    }

    public Event(Object source, T payload) {
        this(source);
        this.payload = payload;
    }

    public T getPayload() {
        return payload;
    }
}
