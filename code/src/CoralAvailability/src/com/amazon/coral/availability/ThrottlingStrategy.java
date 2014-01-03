// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.availability;

/**
 * A {@code ThrottlingStrategy} is a generic interface that allows for simple
 * throttling mechanisms. This allows strategies to be implemented that
 * consider taking action based on service and operation name along with an
 * arbitrary key/value pair.
 *
 * @author Eric Crahen &lt;crahen@amazon.com&gt;
 * @version 1.0
 * @Deprecated prefer Throttler
 */
@Deprecated
public interface ThrottlingStrategy {

  /**
   * This method will examine the information provided and make a decision
   * about weather or not throttling should occur.
   */
  @Deprecated
  public boolean isThrottled(CharSequence key);

}
// Last modified by RcsDollarAuthorDollar on RcsDollarDateDollar
