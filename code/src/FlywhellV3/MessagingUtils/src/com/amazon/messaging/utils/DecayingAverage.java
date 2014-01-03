package com.amazon.messaging.utils;

import javax.annotation.concurrent.GuardedBy;

/**
 * A class that implements a decaying average that resets if the last sample 
 * is too old. The rate of decay is defined by the decay factor (Df). For the 
 * first Df samples the average given will be a simple average. Following that 
 * the n'th average will be calculated as:
 * <center>a<sub>n</sub> = S<sub>n</sub>/Df + ( ( Df - 1 ) / Df ) * a<sub>n-1</sub></center> 
 * where s<sub>n</sub> is the n'th sample. 
 * <p>
 * The initial simple average is needed to make the initial average close to accurate 
 * and not the massive underestimate that would result from using the decaying average
 * formula the entire time.
 * <p>
 * Copied from com.amazon.coral.metrics.reporter.DecayingAverage with the reset if unused
 * feature added
 * 
 * @author stevenso
 * 
 */
public class DecayingAverage {
    private final int decayFactor;
    private final long maximumAge;
    
    private volatile long lastSampleTime;
    private volatile double average;
    
    @GuardedBy("this")
    private int samplesSeen;
   
    public DecayingAverage( int decayFactor, long maximumAge ) {
        this.decayFactor = decayFactor;
        this.maximumAge = maximumAge;
    }
    
    /**
     * Get the current average. Returns Double.NaN if there have not 
     * been any samples reported since currentTime - maximumAge
     * 
     * @param currentTime
     * @return
     */
    public double getAverage(long currentTime) {
        if( currentTime - lastSampleTime > maximumAge ) {
            return Double.NaN;
        }
        return average;
    }
    
    public synchronized double addSample( double sample, long currentTime ) {
        if( currentTime - lastSampleTime > maximumAge ) {
            samplesSeen = 0;
        }
        
        double rate;
        if( samplesSeen < decayFactor ) {
            samplesSeen++;
            rate = 1. / samplesSeen;
        } else {
            rate = 1. / decayFactor;
        }
        
        average = rate * sample + ( 1 - rate ) * average;
        lastSampleTime = currentTime;
        
        return average;
    }
}
    