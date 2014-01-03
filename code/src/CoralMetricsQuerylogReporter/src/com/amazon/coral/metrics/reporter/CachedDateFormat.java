package com.amazon.coral.metrics.reporter;

/**
 * A {@code CachedDateFormat} provides a second resolution cache for
 * {@code DateFormat} instances.
 *
 * @author Matt Wren <wren@amazon.com>
 */
class CachedDateFormat implements DateFormat {

  private final DateFormat format;
  private final ThreadLocal<InternalFormat> internal = new ThreadLocal<InternalFormat>() {
    @Override
    public InternalFormat initialValue() {
      return new InternalFormat();
    }
  };

  /**
   * The InternalFormat object contains the data that is cached per thread.
   */
  private class InternalFormat implements DateFormat {

    private final StringBuilder lastBuffer = new StringBuilder();
    private long last = -1;

    /**
     */
    public void format(StringBuilder buffer, double millisSinceEpoch, CalendarFactory calendarFactory) {

      long s = seconds(millisSinceEpoch);

      if(s != last) {
        last = s;
        lastBuffer.setLength(0);
        format.format(lastBuffer, millisSinceEpoch, calendarFactory);
      }

      buffer.append(this.lastBuffer);

    }

  }

  public CachedDateFormat(DateFormat format) {
    if(format == null)
      throw new IllegalArgumentException();
    this.format = format;
  }

  /**
   */
  public void format(StringBuilder buffer, double millisSinceEpoch, CalendarFactory calendarFactory) {
    this.internal.get().format(buffer, millisSinceEpoch, calendarFactory);
  }

  private long seconds(double millisSinceEpoch) {
    long dateValue = (long)millisSinceEpoch;
    long dateMillis = dateValue % 1000L;
    long dateSeconds = dateValue - dateMillis;
    return dateSeconds;
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
