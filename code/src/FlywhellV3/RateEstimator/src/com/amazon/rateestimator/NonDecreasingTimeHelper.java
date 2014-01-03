package com.amazon.rateestimator;

/**
 * An instance of this class can help you cope with a system clock that moves
 * backwards. It is useful if you don't care about the absolute time, but only
 * the progression of time (and can tolerate a single mistake in that
 * progression each time your clock shifts)
 * 
 */
public class NonDecreasingTimeHelper {

    private double lastInputTime; // the last value of inputTime
    private double lastAdjustedTime; // the last value returned by
                                     // adjustedTime()

    /**
     * Adjust the next value in a sequence of inputTimes so that:
     * <ul>
     * <li>the adjusted sequence never decreases.
     * <li>the difference between every sequential adjusted time-stamps equals
     * the difference between their corresponding input time-stamps, whenever
     * the input time time-stamps are non-decreasing.
     * </ul>
     * 
     * Warning: the value returned by this method may bear no resemblance to the
     * current time! But hey, at least it never goes backwards.
     */
    public double adjustTime(double inputTime) {

        double adjustedTime = this.lastAdjustedTime;

        // ignore time regression, but pass through progress
        if (inputTime > this.lastInputTime) {
            adjustedTime += (inputTime - this.lastInputTime);
        }

        this.lastInputTime = inputTime;
        this.lastAdjustedTime = adjustedTime;
        return adjustedTime;
    }
}
