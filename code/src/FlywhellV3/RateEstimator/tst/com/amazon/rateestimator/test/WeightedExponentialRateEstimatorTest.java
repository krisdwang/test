package com.amazon.rateestimator.test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.amazon.rateestimator.WeightedExponentialRateEstimator;

public class WeightedExponentialRateEstimatorTest {

    // for comparing doubles
    private static final double TOLERANCE = 0.0001;
    
    private double t = 0;
    
    @Test
    public void testLifetimeTotalWeight() {
        final double halfLife = 60;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(halfLife, t);
        assertEquals(0.0, re.getLifetimeTotalWeight());
        re.reportEvent(t, 1000);
        assertEquals(1000.0, re.getLifetimeTotalWeight());
        
        re.reportEvent(t, 500);
        assertEquals(1500.0, re.getLifetimeTotalWeight());
        
        re.reportEvent(t, t+5, 500);
        assertEquals(2000.0, re.getLifetimeTotalWeight());
        
        re.reportNextEvent(20, 250);
        assertEquals(2250.0, re.getLifetimeTotalWeight());
    }

    @Test
    public void testHalfLife() {
        final double halfLife = 5.0;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(halfLife, t);
        doSomeEvents(re, 1000, 100, 1);
    
        // 100 every second for 1000 seconds, yeah the rate is 100/s
        assertEquals(100.0, re.getEstimatedRate(), TOLERANCE);
    
        re.reportEvent(t+halfLife, 0);
        assertEquals(50.0, re.getEstimatedRate(), TOLERANCE);
    
        re.reportEvent(t+2*halfLife, 0);
        assertEquals(25.0, re.getEstimatedRate(), TOLERANCE);
    
        re.reportEvent(t+3*halfLife, 0);
        assertEquals(12.5, re.getEstimatedRate(), TOLERANCE);
        // ...
    }

    @Test
    public void testChangingRates() {
        final double halfLife = 60;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(halfLife, t);

        // start at rate 1000 every 10 seconds => 100/s
        doSomeEvents(re, 100 * halfLife, 1000, 10);
        assertEquals(100.0, re.getEstimatedRate(), TOLERANCE);

        // switch to rate 2000 every 10 seconds
        // -- smoothed rate should be somewhere in between 100 and 200
        doSomeEvents(re, halfLife, 2000, 10);
        assertEquals(150.0, re.getEstimatedRate(), TOLERANCE);

        // continue at rate 2000/10s .. smoothed rate should approach 200
        doSomeEvents(re, 100 * halfLife, 2000, 10);
        assertEquals(200.0, re.getEstimatedRate(), TOLERANCE);
    }

    @Test
    public void testClockGoesForward() {
        final double halfLife = 5.0;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(halfLife, t);
        doSomeEvents(re, 1000, 100, 1);
        assertEquals(100.0, re.getEstimatedRate(), TOLERANCE);
        
        t += 1000000; // uh-oh!
        doSomeEvents(re, 1000, 200, 1);
        
        // yay we recovered!
        assertEquals(200.0, re.getEstimatedRate(), TOLERANCE);
    }
    
    @Test
    public void testMultipleEventsAtOneTime() {
        final double halfLife = 5.0;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(halfLife, t);
        
        // first test that lifetime weight works right: same clock tick (0)
        re.reportEvent(t, 100);
        assertEquals(100.0, re.getLifetimeTotalWeight());
        
        // next test smoothed rate works right
        doSomeEvents(re, 1000, 100, 1);
        assertEquals(100.0, re.getEstimatedRate(), TOLERANCE);
        
        // two reports in one clock tick!
        double deadline = t+1000;
        while (t<deadline) {
            t++;
            re.reportEvent(t, t+1, 100);
            re.reportEvent(t, t+1, 100);
        }
        
        assertEquals(200.0, re.getEstimatedRate(), TOLERANCE);
    }
    
    @Test
    public void testEventsInThePast() {
        
        final double halfLife = 5.0;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(halfLife, t);
        WeightedExponentialRateEstimator re2 = new WeightedExponentialRateEstimator(halfLife, t);
        
        // report to re2 in the natural order
        re.reportEvent(0, halfLife, 100);
        re.reportEvent(halfLife, 2*halfLife, 200);
        
        // report to re2 in the reverse order
        re2.reportEvent(halfLife, 2*halfLife, 200);
        re2.reportEvent(0, halfLife, 100);
        
        // they should have the same estimate
        assertEquals(re.getEstimatedRate(), re2.getEstimatedRate(), TOLERANCE);
        
        // and that estimate should be:
        // - initial rate was 0/halfLife
        // - one halfLife ago, rate was 100/halfLife
        // - most recent halfLife past at rate 200/halfLife
        // Therefore decayed rate will be:
        //    (1/4)*0 + (1/4)*100/halfLife + (1/2)*200/halfLife = 25/halfLife + 100/halfLife
        assertEquals(25/halfLife + 100/halfLife, re2.getEstimatedRate(), TOLERANCE);
    }
    
    @Test
    public void testNoHistoricalEstimates() {
        final double halfLife = 5.0;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(halfLife, t);
        
        re.getEstimatedRate(1);
        
        re.getEstimatedRate(2);
        
        try {
            re.getEstimatedRate(1);
            fail("expected exception");
        } catch (IllegalStateException e) {
            // expected
        }
    }
    
    /**
     * Initialize a rate estimator with 0, and one with a large value.  After that,
     * report events to 2 estimators with the same timestamps and verify that their rates are the same.
     */
    @Test
    public void testDifferentInitialTimes() {
        double halfLife = 5;
        double T = System.nanoTime();
        WeightedExponentialRateEstimator re1 = new WeightedExponentialRateEstimator(halfLife, 0.0, 0);
        WeightedExponentialRateEstimator re2 = new WeightedExponentialRateEstimator(halfLife, T, 0);
        
        int numLoops = 10000;
        double inc = 0.1;
        for (int i = 0; i < numLoops; ++i) {
            T += inc;
            re1.reportEvent(T, 1.0);
            re2.reportEvent(T, 1.0);
            assertEquals(re1.getEstimatedRate(), re2.getEstimatedRate(), TOLERANCE);
        }
    }
    
    @Test
    public void testInitialRateConstructor() {
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(5, t, 100);
        assertEquals(100.0, re.getEstimatedRate(), TOLERANCE);
    }

    @Test
    public void testEventBursts() {
        final double halfLife = 5.0;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(halfLife, t);
        re.reportEvent(t, 100);
        assertEquals(100*Math.log(2)/halfLife,re.getEstimatedRate(), TOLERANCE);
    }
    
    @Test
    public void testClone() {
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(5.0, t);
        re.reportEvent(t, 100);
        
        WeightedExponentialRateEstimator copy = re.duplicate();
        
        // identical!
        assertEquals(re.getEstimatedRate(), copy.getEstimatedRate(), TOLERANCE);
        
        // but now they diverge
        re.reportEvent(t, 100);
        copy.reportEvent(t, 1000);
        
        // confirm independence
        assertTrue(re.getEstimatedRate() > 0);
        assertTrue(copy.getEstimatedRate() > 0);
        assertTrue(re.getEstimatedRate() < copy.getEstimatedRate());
    }
    
    @Test
    public void testReportNextEvent() {
        final double HALF_LIFE = 60;
        WeightedExponentialRateEstimator re = new WeightedExponentialRateEstimator(HALF_LIFE, t);
        
        // run 400m repeats, each taking 80s, for a speed of 5 m/s
        while (re.getLastReportedEventTime() < 20*HALF_LIFE)
            re.reportNextEvent(80, 400);
        assertEquals(5, re.getEstimatedRate(), TOLERANCE);
        
        // now run 800m repeats, each taking 200s, for a speed of 4 m/s
        double start = re.getLastReportedEventTime();
        while (re.getLastReportedEventTime()-start < 20*HALF_LIFE)
            re.reportNextEvent(200, 800);
        assertEquals(4, re.getEstimatedRate(), TOLERANCE);
    }
    
    private void doSomeEvents(WeightedExponentialRateEstimator re, double duration,
            double numUnits, double inc) {
        double deadline = t + duration;
        while (t < deadline) {
            re.reportEvent(t, t+inc, numUnits);
            t += inc;
        }
    }
}
