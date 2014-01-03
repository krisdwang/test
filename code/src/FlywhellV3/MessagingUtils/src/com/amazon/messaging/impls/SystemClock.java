package com.amazon.messaging.impls;

import com.amazon.messaging.interfaces.Clock;

/**
 * A clock that just returns the current system time
 * @author stevenso
 *
 */
public class SystemClock implements Clock {
    public static final SystemClock instance = new SystemClock();
    
    @Override
    public long getCurrentTime() {
        return System.currentTimeMillis();
    }

}
