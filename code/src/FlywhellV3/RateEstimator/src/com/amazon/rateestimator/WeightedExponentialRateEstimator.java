package com.amazon.rateestimator;

/**
 * The class estimates the instantaneous rate of a stream of weighted events. It
 * uses exponential smoothing to bias the estimate towards the recent arrival
 * rate.
 * 
 * Example uses:
 * <ul>
 * <li>How many messages per second is my server processing? (Call
 * reportEvent(1) every time you process a message)
 * <li>How many bytes per second is my upload job doing? (Call
 * reportEvent(bufSize) every time your write() system call returns)
 * <li>...
 * </ul>
 * 
 * This class doesn't care what units you use for time or for weight. The
 * estimated rate will come out in your units of weight per your units of time.
 * 
 * There are a two opposing issues to consider when choosing a value for
 * halfLife. The right value will be a trade-off between the two:
 * <ul>
 * <li>How fast does the estimator adapt to change? If the rate changes abruptly
 * from P to Q then the estimate is alpha*P + (1-alpha)*Q, where alpha is
 * (1/2)^n, where n is the number of half lives that happened since the change.
 * So you get an additional bit of accuracy for each half life that has gone by.
 * So after 6 half lives, the first six bits of your estimate are Q, and the
 * rest are P, you have about 98.5% accuracy (since 1/64 = 0.015).
 * 
 * <li>How much error is there in the steady state? If bursts of events are
 * reported with a period of T, then the error in the estimate is bounded by
 * (1/2)^(T/h). So you gain a bit of accuracy each time you increase half life
 * by T. (However, if the bursts are not really part of the data stream but just
 * an artifact of how you are observing it, then check out the version of
 * reportEvent() that takes a time interval. This can really smooth things out)
 * </ul>
 * 
 * You are free to report events in any order, even later events before earlier
 * ones, but getEstimatedRate() always returns the most up-to-date estimate of
 * the rate calculated as of the largest value of time provided to this instance
 * by any constructor or method. When you report an event in the "past" (an
 * eventTime smaller than the largest eventTime ever seen) the estimate will be
 * updated to take this into account.
 * 
 * Problem: RateEstimator foolishly depends on you to keep time marching 
 * forward. If you tell it about a packet that arrived with the current time, 
 * then turn around and report that another packet arrived at midnight January 
 * 1, 1970, it won't even blink an eye, much less suspect that something is 
 * wrong with your clock.
 * 
 * Unfortunately, real-world clocks do tend to jump around a lot. For many
 * different reasons, time may go forward much faster than in reality, or may
 * jump backwards. So if, for example, you blindly pass the result of
 * System.currentTimeMillis() to this RateEstimator, and your system clock
 * shifts abruptly, you should know that your rate estimates could get messed 
 * up in a way that may not correct itself for a long long time:
 * <ul>
 * <li>If your clock jumps way backwards, subsequent calls to reportEvent() will
 * be in the distant past from the RateEstimator's perspective, and so it will
 * decay them so much as to be insignificant. These ancient events will not move
 * your estimated rate much until the time according to your newly shifted clock
 * catches up to its nominal value under the old clock. In other words, your
 * rate estimate could be frozen at its current value for a very long time.
 * <li>If your clock jumps way forward, your rate estimate will go to zero as
 * the RateEstimator decays all recent usage into oblivion. If your clock stays
 * put, your estimate will re-converge to the true rate as quickly as half-life
 * allows. This is good. However if your clock corrects itself by moving back
 * again, you are stuck with the problem mentioned above.
 * </ul>
 * The best way to handle this problem is to use a source of time that never
 * goes backwards, even if your underlying computer clock does.
 * 
 * @see NonDecreasingTimeHelper
 * 
 */
public class WeightedExponentialRateEstimator {
    /*
     * The sum of the weights of all the events ever reported, decayed according
     * to their age.
     */
    private double totalDecayedWeight;

    // currentDecayedWeight decays at a rate of -lambda times its current value.
    // equal to ln(2)/halfLife
    private final double lambda;

    // when currentDecayedWeight was last valid
    private double timeValid;

    // sum of all weight reported ever
    private double lifetimeTotalWeight;

    /**
     * Create a new WeightedExponentialRateEstimator with initial rate of zero.
     * 
     * @see #WeightedExponentialRateEstimator(double halfLife, double currentTime, double initialRate)
     * 
     * @param halfLife
     *            How much weight should recent events be given compared to
     *            events in the distant past?
     * @param currentTime
     *            What time is it? (you pick the units)
     */
    public WeightedExponentialRateEstimator(double halfLife, double currentTime) {
        this(halfLife, currentTime, 0);
    }

    /**
     * Create a new WeightedExponentialRateEstimator with the specified
     * parameters.
     * 
     * Half life is a parameter that selects how quickly the smoothed rate
     * adapts to recent changes. Or in other words, how much weight recent
     * events are given compared to events in the distant past. The best way to
     * get a feel for half life is to consider a stream of events that is
     * progressing at a certain rate X, and then suddenly stops. After halfLife
     * units of time, the smoothed rate will be X/2. After 2*halfLife units of
     * time, the smoothed rate will be X/4 ... and so on.
     * 
     * @param halfLife
     *            How long before past events account for only half of the
     *            current estimate?
     * @param currentTime
     *            What time is it? (you pick the units)
     * @param initialRate
     *            Choose an initial rate for the stream (you pick the units)
     */
    public WeightedExponentialRateEstimator(double halfLife,
                                            double currentTime,
                                            double initialRate) {
        if (halfLife <= 0)
            throw new IllegalArgumentException("Invalid half life: " + halfLife);
        this.lambda = Math.log(2) / halfLife;
        this.timeValid = currentTime;
        this.totalDecayedWeight = initialRate / lambda;
    }
    
    private WeightedExponentialRateEstimator(double lambda, double timeValid, double totalDecayedWeight, @SuppressWarnings("unused") boolean dummy) {
        this.lambda = lambda;
        this.timeValid = timeValid;
        this.totalDecayedWeight = totalDecayedWeight;
    }

    // age the current estimate - no events between timeValid and now.
    // Every time unit, the current estimated rate decreases by a factor
    // of lambda due to the exponential decay.
    private void maybeAge(double eventTime) {
        if (eventTime > timeValid) {
            totalDecayedWeight *= Math.exp(-lambda * (eventTime - timeValid));
            timeValid = eventTime;
        }
    }

    /**
     * Update the rate estimate to include a new instantaneous event at the
     * specified time and of the specified weight.
     * 
     * You should use this form of reportEvent() when you are observing
     * events that happen at a single point in time, for example:
     * <ul>
     * <li>a customer clicks on an advertisement.
     * <li>a message arrives on the network.
     * <li>a new UNIX process is created.
     * </ul>
     * 
     * @param eventTime
     *            When did the event happen? (you pick the units)
     * @param weight
     *            The weight of this event (you pick the units)
     */
    public void reportEvent(double eventTime, double weight) {
        maybeAge(eventTime);
        totalDecayedWeight += weight
                                * Math.exp(-lambda * (timeValid - eventTime));
        lifetimeTotalWeight += weight;
    }

    /**
     * Update the rate estimate to include a new event that happened
     * with the specified weight and over the specified period of time.
     * 
     * You can use this form of reportEvent() when you'd rather hear about 
     * batches of events that occurred over a period of time.
     * <ul>
     * <li>1532 cars have passed over your traffic sensor since
     * you last checked an hour ago.
     * <li>A 17M download finishes after 63 seconds.
     * <li>Your server has processed 1000 requests since its last report.
     * </ul>
     * 
     * This method assumes that between eventStartTime and eventEndTime, the
     * event proceeded at a uniform rate. If eventStartTime and eventEndTime
     * are the same, then this is equivalent to calling reportEvent with
     * one time.
     * 
     * @param eventStartTime
     *            When the event started (you pick the time units)
     * @param eventEndTime
     *            When the event finished (you pick the time units)
     * @param weight
     *            The weight of this event (you pick the units)
     */
    public void reportEvent(double eventStartTime, double eventEndTime,
                            double weight) {
        double eventDuration = eventEndTime - eventStartTime;
        if (eventDuration < 0) {
            throw new IllegalArgumentException();
        }
        maybeAge(eventEndTime);
        
        // the stretch factor discounts the weight so that it's contribution to the
        // decayed weight is the same as an instantaneous event that happened at eventEndTime
        double stretchFactor = 1;
        if (eventDuration > 0)
            stretchFactor = ( 1 - Math.exp( -lambda*eventDuration ) ) / ( lambda * eventDuration );
        
        lifetimeTotalWeight += weight;
        totalDecayedWeight += stretchFactor * weight * Math.exp(-lambda * (timeValid - eventEndTime));
    }

    /**
     * Update the rate estimate to include a new event that happened with the
     * specified weight, that took the specified duration, and that started
     * happening right after the last reported event.
     * 
     * The idea is that when you have a sequence of (duration,weight) pairs, you
     * can report them without having to keep an explicit clock. For example you
     * might use this to estimate the velocity of a runner doing repeats. Each
     * time the runner starts a repeat, you start your stopwatch. When the
     * runner finishes, you stop the watch, and report (duration,distance). This
     * strategy ignores the down time between repeats. You can use this to estimate
     * the maximum throughput of some process which has to sometimes wait for work.
     * 
     * @param duration
     *            How long this event took (you pick the time units)
     * @param weight
     *            The weight of this event (you pick the units)
     */
    public void reportNextEvent(double duration, double weight) {
        if (duration<0)
            throw new IllegalArgumentException();
        
        maybeAge( this.timeValid + duration );
        
        // the stretch factor discounts the weight so that it's contribution to the
        // decayed weight is the same as an instantaneous event
        double stretchFactor = 1;
        if (duration > 0)
            stretchFactor = ( 1 - Math.exp( -lambda*duration ) ) / ( lambda * duration );
        
        lifetimeTotalWeight += weight;
        totalDecayedWeight += stretchFactor * weight;
    }

    /**
     * Fetch the estimated weight per unit time as of "currentTime"
     * 
     * If currentTime is larger than the latest event ever reported, we return
     * the estimated rate as of currentTime, and assume that zero events took
     * place since then.
     * 
     * You cannot use this method to learn a historical value for the rate
     * estimate. If currentTime is less than or equal to the time of the latest
     * event ever reported, we throw IllegalStateException.
     * 
     * @param currentTime
     *            The time you want to roll-forward to, and estimate at
     * 
     * @return the estimated rate as of "currentTime"
     * @throws IllegalStateException if you try to get a rate in the past
     */
    public double getEstimatedRate(double currentTime) {
        if (currentTime < timeValid) {
            throw new IllegalStateException(
                    "You asked for the estimated rate at t=" + currentTime
                  + " which is earlier than your last reportEvent()" 
                  + " which was at" + timeValid);
        }
        // assume nothing else happened up until currentTime
        reportEvent(currentTime, 0);
        return getEstimatedRate();
    }

    /**
     * Fetch the most up-to-date estimate of the weight per unit time.
     * 
     * If time has past since you last called reportEvent() and you know that no
     * events have happened since then, you probably want to first call
     * reportEvent(currentTime, 0) in order to take that information into
     * account. Or just use the getEstimatedRate(currentTime) short-cut.
     * 
     * @return the most up-to-date estimate of the weighted rate of events
     */
    public double getEstimatedRate() {
        return totalDecayedWeight * lambda;
    }

    /**
     * Returns the sum of the weights of all reportEvent() calls ever.
     * 
     * @return the sum of the weights of all reportEvent() calls ever.
     */
    public double getLifetimeTotalWeight() {
        return this.lifetimeTotalWeight;
    }
    
    /**
     * Returns the time of the last reported event.
     */
    public double getLastReportedEventTime() {
        return this.timeValid;
    }

    /**
     * Fetch a String representation of this object, for human consumption.
     */
    @Override
    public String toString() {
        return Double.toString(getEstimatedRate());
    }
    
    /**
     * Make a new RateEstimator that shares this one's history, but whose 
     * future is yet to be decided! 
     * 
     * The copy is independent: events reported to it will not be reflected 
     * in this.getEstimatedRate() and vice versa.
     * 
     * @return an independent copy of this estimator
     */
    public WeightedExponentialRateEstimator duplicate() {
        return new WeightedExponentialRateEstimator(lambda, timeValid, totalDecayedWeight, true);
    }
}
