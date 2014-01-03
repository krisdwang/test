package com.amazon.coral.spring.director.live;

/**
 * Enumerates the names of Live-compatible events.
 * <p>
 * ForkJoin-s can be configured to dispatch NamedEvent-s with String payloads that correspond to .toString() conversions
 * of this enum's elements. If that happens, LiveAdapter will know how to route the events to their namesake methods.
 * </p>
 * 
 * @author Ovidiu Gheorghies
 */
public enum LiveEvent {
    acquire,
    activate,
    deactivate,
    release;
}
