// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

import java.util.BitSet;

/**
 * A {@code TimeDecayHealth} instance is responsible for keeping track of the
 * perceived health of a service.
 *
 * This is based on:
 * //brazil/src/shared/platform/JavaServiceAvailabilityManager/release/javaServiceAvailabilityManager/amazon/platform/sam/statistics/COLTHealthStat.java
 *
 * Note: this object is not thread safe. Synchronization will be done at the caller level (e.g. TimeDecayBackoffHandler).
 *
 * @author Ameet Vaswani <vaswani@amazon.com>
 * @author Eric Crahen <crahen@amazon.com>
 * @version 1.0
 * @deprecated Nobody is using this.
 */
@Deprecated
public class TimeDecayHealth {

  public final static double GreenHealth = 1.0;
  public final static double YellowHealth = 0.5;
  public final static double RedHealth = 0.0;

  private final static double YellowThresholdTti_ = 3.0;
  private final static double RedThresholdTti_ = 10.0;
  private final static double YellowThresholdLti_ = 1.0;
  private final static double RedThresholdLti_ = 2.0;
  private final static double YellowThresholdTimeoutRate_ = 0.5;
  private final static double RedThresholdTimeoutRate_ = 0.8;
  private final static double timeDecayFactor_ = 0.1;
  private final static double shortDuration_ = 60;
  private final static double longDuration_ = 300;
  private final static double troubleIndicatorDecayPeriod_ = 120;
  private final static int timeoutBitsetSize_ = 32;

  private double shortTermLatency_;
  private double longTermLatency_;
  private double latencyTroubleIndicator_;
  private long lastLtiUpdateTimestamp_;

  private double shortTermTimeoutRate_;
  private double longTermTimeoutRate_;
  private double timeoutTroubleIndicator_;
  private long lastTtiUpdateTimestamp_;

  private final BitSet timeoutBitset_ = new BitSet();

  private int currentTimeoutBitsetIndex_;

  protected long getTimeInSeconds() {
    return System.currentTimeMillis() / 1000;
  }

  public double getHealth() {

    double health;
    if ( latencyTroubleIndicator_ > RedThresholdLti_ ||
        timeoutTroubleIndicator_ > RedThresholdTti_ ||
        shortTermTimeoutRate_ > RedThresholdTimeoutRate_ )
    {
      health = RedHealth;
    }
    else if ( latencyTroubleIndicator_ > YellowThresholdLti_ ||
        timeoutTroubleIndicator_ > YellowThresholdTti_ ||
        shortTermTimeoutRate_ > YellowThresholdTimeoutRate_ )
    {
      health = YellowHealth;
    }
    else
    {
      health = GreenHealth;
    }

    return health;
  }


  private void updateLatencyTroubleIndicator(double lastLatency) {

    long currentTime = getTimeInSeconds();

    if(lastLtiUpdateTimestamp_ == 0)  {
      shortTermLatency_ = lastLatency;
      longTermLatency_ = lastLatency;
    } else {
      double shortTermDecayFactor = Math.pow( timeDecayFactor_, (double)(currentTime - lastLtiUpdateTimestamp_) / shortDuration_ );
      shortTermLatency_ = shortTermLatency_ * shortTermDecayFactor + lastLatency * ( 1 - shortTermDecayFactor );
      double longTermDecayFactor = Math.pow( timeDecayFactor_, (double)(currentTime - lastLtiUpdateTimestamp_) / longDuration_ );
      longTermLatency_ = longTermLatency_ * longTermDecayFactor + lastLatency * ( 1 - longTermDecayFactor );
    }

    double troubleIndicatorDecayFactor = Math.pow( timeDecayFactor_, (double)(currentTime - lastLtiUpdateTimestamp_) / troubleIndicatorDecayPeriod_ );
    double newLatencyTroubleIndicator = calculateFirstDerivative( longTermLatency_, shortTermLatency_ );
    latencyTroubleIndicator_ = latencyTroubleIndicator_ * troubleIndicatorDecayFactor + newLatencyTroubleIndicator * ( 1 - troubleIndicatorDecayFactor );

    lastLtiUpdateTimestamp_ = currentTime;

  }

  private void updateTimeoutTroubleIndicator() {

    long currentTime = getTimeInSeconds();
    double currentTimeoutRate = calculateCurrentTimeoutRate();

    if(lastTtiUpdateTimestamp_ == 0) {
      shortTermTimeoutRate_ = currentTimeoutRate;
      longTermTimeoutRate_ = currentTimeoutRate;
    } else {
      double shortTermDecayFactor = Math.pow( timeDecayFactor_, (double)(currentTime - lastTtiUpdateTimestamp_) / shortDuration_ );
      shortTermTimeoutRate_ = shortTermTimeoutRate_ * shortTermDecayFactor + currentTimeoutRate * ( 1 - shortTermDecayFactor );
      double longTermDecayFactor = Math.pow( timeDecayFactor_, (double)(currentTime - lastTtiUpdateTimestamp_) / longDuration_ );
      longTermTimeoutRate_ = longTermTimeoutRate_ * longTermDecayFactor + currentTimeoutRate * ( 1 - longTermDecayFactor );
    }

    double troubleIndicatorDecayFactor = Math.pow( timeDecayFactor_, (double)(currentTime - lastTtiUpdateTimestamp_) / troubleIndicatorDecayPeriod_ );
    double newTimeoutTroubleIndicator = calculateFirstDerivative( longTermTimeoutRate_, shortTermTimeoutRate_ );
    timeoutTroubleIndicator_ = timeoutTroubleIndicator_ * troubleIndicatorDecayFactor + newTimeoutTroubleIndicator * ( 1 - troubleIndicatorDecayFactor );
    lastTtiUpdateTimestamp_ = currentTime;

  }

  private void updateTimeoutRateCalculator(boolean isTimeout) {

    timeoutBitset_.set(currentTimeoutBitsetIndex_, isTimeout);
    currentTimeoutBitsetIndex_++;
    if (currentTimeoutBitsetIndex_ == timeoutBitsetSize_)
      currentTimeoutBitsetIndex_ = 0;

  }

  private double calculateCurrentTimeoutRate() {
    return (double)timeoutBitset_.cardinality() / timeoutBitsetSize_;
  }

  /**
  * Record latency for a "successful" call (one that didn't time out).
  */
  public void recordLatency(double lastLatency) {
     updateLatencyTroubleIndicator(lastLatency);
     updateTimeoutRateCalculator(false);
     updateTimeoutTroubleIndicator();
  }

  /**
   * Record timeout an unsuccessful call.
   */
  public void recordTimeout() {
    updateTimeoutRateCalculator(true);
    updateTimeoutTroubleIndicator();
  }

  private double calculateFirstDerivative(double v0, double v1) {
    double ti = 0.0;
    if ( v1 > v0 ) {
      if (v0 == 0) {
        ti = v1;
      } else {
        ti = (v1 - v0)/v0;
      }
    }
    return ti;
  }

}
