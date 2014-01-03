package com.amazon.messaging.utils;

import java.util.concurrent.TimeUnit;

import com.amazon.messaging.impls.NanoTimeBasedClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.rateestimator.WeightedExponentialRateEstimator;

/**
 * A utility class to estimate the rate of operations via using provided clock.
 * 
 * @author Kaloian Manassiev (kalman@amazon.com)
 */
public class RateEstimator {

    // Clock shared between all instances
    //
    private static final  Clock DEFAULT_CLOCK = new NanoTimeBasedClock();

    private static final double NANOS_PER_SECOND = (double) TimeUnit.SECONDS.toNanos(1);

    private static final double HALF_LIFE_SECONDS = 10.0; // Ten seconds interval of decay

    private final WeightedExponentialRateEstimator rateEstimator;

    private final double clockDivisor;

    private final Clock clock;
    
    /** By default, estimates are per second, and half-life is 10 seconds. */
    public RateEstimator() {
        clockDivisor = NANOS_PER_SECOND;
        clock = DEFAULT_CLOCK;
        rateEstimator = new WeightedExponentialRateEstimator(HALF_LIFE_SECONDS, getCurrentTime());
    }

    public RateEstimator(double halfLife, double clockDivisor, Clock clock) {
        this.clockDivisor = clockDivisor ;
        this.clock = clock;
        rateEstimator = new WeightedExponentialRateEstimator(halfLife, getCurrentTime());
    }

    public synchronized void reportEvent() {
        rateEstimator.reportEvent(getCurrentTime(), 1.0);
    }
    
    public synchronized void reportEvent(double eventWeight) {
        rateEstimator.reportEvent(getCurrentTime(), eventWeight);
    }

    public synchronized double getEstimatedRate() {
        // Not using rateEstimator.getEstimatedRate( getCurrentTime() ) as that 
        // was complaining about time going backwards(probably due to rounding somewhere)
        rateEstimator.reportEvent(getCurrentTime(), 0);
        return rateEstimator.getEstimatedRate();
    }

    private double getCurrentTime() {
        return (((double) clock.getCurrentTime()) / clockDivisor);
    }
}
