package com.amazon.coral.metrics.reporter;

import java.util.Calendar;

/**
 * SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
 * @see http://en.wikipedia.org/wiki/ISO_8601
 *
 * @author Matt Wren <wren@amazon.com>
 */
class ISO8601Format implements DateFormat {

  /**
   * Given a time in UTC milliseconds from the epoch, format it into a
   * StringBuilder using the ISO8601 format and the TimeZone associated
   * with this format object.
   */
  public void format(StringBuilder buffer, double millisSinceEpoch, CalendarFactory calendarFactory) {

    Calendar calendar = calendarFactory.newCalendar();
    calendar.setTimeInMillis((long)millisSinceEpoch);

    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1; // stored in zero-base (January=0)
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    int minute = calendar.get(Calendar.MINUTE);
    int second = calendar.get(Calendar.SECOND);

    int tzTotalMinutes = (calendar.get(Calendar.ZONE_OFFSET) + calendar.get(Calendar.DST_OFFSET)) / (60 * 1000);

    boolean tzNegative = (tzTotalMinutes < 0);

    int tzHours = Math.abs(tzTotalMinutes / 60);
    int tzMinutes = Math.abs(tzTotalMinutes % 60);

    buffer.append(year);
    buffer.append('-');

    if(month < 10)
      buffer.append('0');
    buffer.append(month);
    buffer.append('-');

    if(day < 10)
      buffer.append('0');
    buffer.append(day);
    buffer.append('T');

    if(hour < 10)
      buffer.append('0');
    buffer.append(hour);
    buffer.append(':');

    if(minute < 10)
      buffer.append('0');
    buffer.append(minute);
    buffer.append(':');

    if(second < 10)
      buffer.append('0');
    buffer.append(second);

    if(tzNegative)
      buffer.append('-');
    else
      buffer.append('+');

    if(tzHours < 10)
      buffer.append('0');
    buffer.append(tzHours);
    if(tzMinutes < 10)
      buffer.append('0');
    buffer.append(tzMinutes);

  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
