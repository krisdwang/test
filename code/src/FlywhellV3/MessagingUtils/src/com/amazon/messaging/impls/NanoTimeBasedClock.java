package com.amazon.messaging.impls;



import com.amazon.messaging.interfaces.Clock;



/**
 * An always-increasing nanosecond resolution clock, based on the {@link System#nanoTime()} call.
 */
public final class NanoTimeBasedClock implements Clock {

    private final long base = System.nanoTime ();

    @Override
    public long getCurrentTime () {
        return System.nanoTime () - base;
    }
}
