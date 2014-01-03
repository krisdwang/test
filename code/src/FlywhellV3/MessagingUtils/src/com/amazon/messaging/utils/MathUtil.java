package com.amazon.messaging.utils;


public class MathUtil {
    /**
     * Adds two numbers together clip the result at Long.MAX_VALUE if it gets too large.
     * 
     * @return first + second or Long.MAX_VALUE if first + second is to large to fit in a long
     */
    public static long addNoOverflow(long first, long second) {
        long result = first + second;
        if( result < 0 ) result = Long.MAX_VALUE;
        
        return result;
    }
}
