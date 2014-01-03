package com.amazon.coral.metrics.reporter;

/**
 * DoubleFormat provides a performance-oriented double-to-String conversion.
 *
 * The JavaAPI provides several methods to convert double values to Strings;
 * none of them are particularly fast.  A simple benchmark that illustrates
 * this is provided in the TestDoubleFormat class,  showing that the code
 * in this class is more than 5x faster than the API's best:
 *
 * DoubleFormat Performance comparison: 1000000 iterations
 *     Java StringBuilder: 1444 ms
 *     Java NumberFormat:  2093 ms
 *     DoubleFormat:       257 ms
 *
 * This class loosely follows the interface design of java.text.NumberFormat
 *
 * @author Matt Wren <wren@amazon.com>
 */
class DoubleFormat {

  // Lookup table for powers of 10
  private final static double[] POW_10 = new double[]{
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
  };
  // Whole number value beyond which this formatter cannot function
  // (Will fallback to slower Java API)
  private final static double TOO_BIG_THRESHOLD = (double)(0x7FFFFFFF);


  private int minimumFractionDigits = 0;
  // max value for maximumFractionDigits reduced from 9 to 8; 9 causes integer overflows in countLeadingZeros().
  private int maximumFractionDigits = 8;

  /**
   * Sets the minimum number of digits allowed in the fraction portion of a number.
   */
  public void setMinimumFractionDigits(int digits) {
    if(digits < 0 || digits > 8)
      throw new IllegalArgumentException("Digits value must be within [0,8]");

    minimumFractionDigits = digits;
  }

  /**
   * Sets the maximum number of digits allowed in the fraction portion of a number.
   */
  public void setMaximumFractionDigits(int digits) {
    if(digits < 0 || digits > 8)
      throw new IllegalArgumentException("Digits value must be within [0,8]");

    maximumFractionDigits = digits;
  }

  /**
   * Sets the minimum number of digits allowed in the fraction portion of a number.
   */
  public int getMinimumFractionDigits() {
    return minimumFractionDigits;
  }

  /**
   * Sets the maximum number of digits allowed in the fraction portion of a number.
   */
  public int getMaximumFractionDigits() {
    return maximumFractionDigits;
  }

  /**
   * Formats a double and appends it to an existing StringBuilder object.
   */
  public void format(StringBuilder sb, double value) {

    if(maximumFractionDigits < minimumFractionDigits)
      throw new IllegalStateException("maximumFractionDigits must not be less than minimumFractionDigits");

    // store the sign of the input, get the input as a positive number w/o sign
    boolean negative = (value < 0);
    double positiveValue = Math.abs(value);

    // if value is too large for us to handle, revert to builtin formatter
    if(positiveValue > TOO_BIG_THRESHOLD) {
      handleInputTooBig(sb, value);
      return;
    }

    // Separate the number into whole number and fraction components.
    // Scale the fraction component based upon the max number of digits to display.
    // Additionally, store remaining (unprinted) fraction amount for use in rounding
    // decisions.
    int whole = (int) positiveValue;
    double tmp = (positiveValue - whole) * POW_10[maximumFractionDigits];
    int fraction = Math.abs((int) tmp);
    double diff = tmp - fraction;

    if(diff > 0.5) {
      // round up
      fraction++;
    } else if(diff == 0.5 && ((fraction == 0) || (fraction & 1) != 0)) {
      // round up
      fraction++;
    }

    // if rounding has pushed us up to the next whole number, adjust:
    // clear fraction amount and increment whole number
    if(fraction >= POW_10[maximumFractionDigits]) {
      fraction = 0;
      whole++;
    }

    int leadingZeros = countLeadingZeros(fraction);
    fraction = trimTrailingZeros(fraction);

    appendRoundedValue(sb, negative, whole, leadingZeros, fraction);
  }

  // Once the double value has been appropriately split and rounded,
  // this function writes it to the StringBuilder in the proper format.
  private StringBuilder appendRoundedValue(StringBuilder sb, boolean negative, int whole, int leadingZerosInFraction, int fraction) {
    if(negative)
      sb.append('-');
    sb.append(whole);
    if(fraction != 0 || minimumFractionDigits > 0) {
      sb.append('.');

      for(int i = 0; i < leadingZerosInFraction; i++)
        sb.append('0');

      sb.append(fraction);
    }
    return sb;
  }

  // Counts the number of digits between the decimal point and the first non-zero
  // in the fraction.  As this is done in integer arithmetic, we simply move the
  // decimal point right one place at a time until the fraction exceeds '1' in
  // the space of the original number (before splitting whole from fraction).
  private int countLeadingZeros(int fraction) {
    int max = (int) POW_10[maximumFractionDigits];

    // detect this error because otherwise you get infinite looping...
    if(fraction < 0)
      throw new IllegalArgumentException("Negative fraction: " + fraction);

    // if the fraction is zero, specify minimumFractionDigits-1 as the zero count
    // otherwise no fraction will be displayed.
    if(fraction == 0)
      return minimumFractionDigits - 1;

    int count = 0;

    while( (fraction *= 10) < max ) {
      count++;
    }

    return count;
  }

  // Removes trailing zeros from the fraction up to the maximum number permitted
  // by the minimumFractionDigits value.
  private int trimTrailingZeros(int fraction) {
    int maxToTrim = maximumFractionDigits - minimumFractionDigits;

    for(int i = 0; i < maxToTrim; i++) {
      if(fraction % 10 != 0)
        return fraction;
      else
        fraction /= 10;
    }

    return fraction;
  }

  // DoubleFormat can only handle values whose whole number component can be
  // stored in a 32-bit integer.  Anything larger than this must fallback to
  // the java API's formatter, which is much slower.
  private void handleInputTooBig(StringBuilder sb, double value) {
    java.text.NumberFormat format = java.text.NumberFormat.getInstance();
    format.setMinimumFractionDigits(minimumFractionDigits);
    format.setMaximumFractionDigits(maximumFractionDigits);
    format.setGroupingUsed(false);

    sb.append(format.format(value));
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
