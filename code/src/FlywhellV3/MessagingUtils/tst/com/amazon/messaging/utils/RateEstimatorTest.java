package com.amazon.messaging.utils;



import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.amazon.messaging.impls.NanoTimeBasedClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.testing.TestScheduledExecutorService;



public class RateEstimatorTest {

    private final Log log = LogFactory.getLog (RateEstimatorTest.class);

    private TestScheduledExecutorService executor;

    @Before
    public void before () {
        executor = new TestScheduledExecutorService ("Executor", 16);
    }

    @After
    public void after () throws Exception {
        log.trace ("Test completed, destroying executor.");

        executor.shutdown ();
        executor.awaitTermination (Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        executor.rethrow ();
    }

    @Test
    public void nanoTimeNeverGoesBackTest () {
        final SynchronizedClock clock = new SynchronizedClock (new NanoTimeBasedClock ());
        for (int i = 0; i < 16; i++) {
            executor.execute (new Runnable () {
                @Override
                public void run () {
                    for (int i = 0; i < 200000; i++) {
                        clock.getCurrentTime ();
                    }
                }
            });
        }
    }

    @Test @Ignore("ignored for now as we've worked around this")
    public void nanoTimeRoundingErrorTest () {
        final DoubleClock clock = new DoubleClock ();
        for (int i = 0; i < 16; i++) {
            executor.execute (new Runnable () {
                @Override
                public void run () {
                    for (int i = 0; i < 200000; i++) {
                        clock.updateTime ();
                    }
                }
            });
        }
    }

    @Test @Ignore("ignored for now as we've worked around this")
    public void nanoTimeRoundingErrorAndTimeNeverGoesBackTest () {
        final DoubleClock clock = new DoubleClock (new SynchronizedClock (new NanoTimeBasedClock ()));
        for (int i = 0; i < 16; i++) {
            executor.execute (new Runnable () {
                @Override
                public void run () {
                    for (int i = 0; i < 200000; i++) {
                        clock.updateTime ();
                    }
                }
            });
        }
    }

    @Test
    public void concurrencyRateEstimatorTest () throws InterruptedException {
        final RateEstimator estimator = new RateEstimator ();

        for (int i = 0; i < 16; i++) {
            executor.execute (new Runnable () {
                @Override
                public void run () {
                    for (int i = 0; i < 100000; i++) {
                        estimator.getEstimatedRate ();
                        estimator.reportEvent ();
                    }
                }
            });
        }

        executor.shutdown ();
        executor.awaitTermination (Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    private static final class SynchronizedClock implements Clock {

        private final Clock clock;
        private long currentTime;

        public SynchronizedClock (Clock clock) {
            this.clock = clock;
            currentTime = clock.getCurrentTime ();
        }

        @Override
        public synchronized long getCurrentTime () {
            long newTime = clock.getCurrentTime ();
            if (newTime < currentTime) {
                throw new IllegalStateException ("Expected " + newTime + " to be greater than the old time of "
                        + currentTime);
            }
            return newTime;
        }

    }

    private static final class DoubleClock {
        private static final Clock DEFAULT_CLOCK = new NanoTimeBasedClock ();

        private static final double NANOS_PER_SECOND = (double) TimeUnit.SECONDS.toNanos (1);

        private final double clockDivisor;

        private final Clock clock;

        private long currentTimeLong;
        private double currentTimeDouble;

        public DoubleClock () {
            clockDivisor = NANOS_PER_SECOND;
            clock = DEFAULT_CLOCK;

            updateTime ();
        }

        public DoubleClock (Clock clock) {
            clockDivisor = NANOS_PER_SECOND;
            this.clock = clock;

            updateTime ();
        }

        public synchronized void updateTime () {
            long newTimeLong = clock.getCurrentTime ();
            double newTimeDouble = ((double) newTimeLong) / clockDivisor;

            if (newTimeLong < currentTimeLong) {
                throw new IllegalStateException ("Long mismatch: expected [" + newTimeLong + "," + newTimeDouble
                        + "] to be greater than the old time of " + currentTimeLong + "," + currentTimeDouble + "]");
            }

            if (newTimeDouble < currentTimeDouble) {
                throw new IllegalStateException ("Double mismatch: expected [" + newTimeLong + "," + newTimeDouble
                        + "] to be greater than the old time of " + currentTimeLong + "," + currentTimeDouble + "]");
            }

            currentTimeLong = newTimeLong;
            currentTimeDouble = newTimeDouble;
        }
    }
}
