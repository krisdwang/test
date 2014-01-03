package com.amazon.coral.spring.director.event;

/**
 * A Spring event with a payload of type String.
 * 
 * @author Ovidiu Gheorghies
 */
public class NamedEvent extends Event<String> {

    public NamedEvent(Object source, String payload) {
        super(source, payload);

        if (payload == null) {
            throw new IllegalArgumentException("Name of NamedEvent must not be null");
        }
    }

    /**
     * 
     */
    private static final long serialVersionUID = 7387389746039738726L;

    @Override
    public String toString() {
        return "{" + payload + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof NamedEvent)) {
            return false;
        }

        NamedEvent cmp = (NamedEvent) o;

        // source, payload are not null per constructor preconditions
        return source.equals(cmp.source) && payload.equals(cmp.payload);
    }

    @Override
    public int hashCode() {
        return source.hashCode() + payload.hashCode();
    }
}
