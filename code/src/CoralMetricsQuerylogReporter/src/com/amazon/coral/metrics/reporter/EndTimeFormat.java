package com.amazon.coral.metrics.reporter;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * java.text.SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z")
 *
 * @author Matt Wren <wren@amazon.com>
 */
class EndTimeFormat implements DateFormat {

  /**
   * Given a time in UTC milliseconds from the epoch, format it into a
   * StringBuilder using the EndTime format and the TimeZone associated
   * with this format object.
   */
  public void format(StringBuilder buffer, double millisSinceEpoch, CalendarFactory calendarFactory) {

    Calendar calendar = calendarFactory.newCalendar();
    calendar.setTimeInMillis((long)millisSinceEpoch);

    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1; // stored in zero-base (January=0)
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    int minute = calendar.get(Calendar.MINUTE);
    int second = calendar.get(Calendar.SECOND);

    boolean isDaylightSavings = calendar.get(Calendar.DST_OFFSET) != 0;

    String strTimeZone = calendar.getTimeZone().getDisplayName(isDaylightSavings,TimeZone.SHORT);
    String strDayOfWeek = getDayName(dayOfWeek);
    String strMonth = getMonthName(month);

    buffer.append(strDayOfWeek);
    buffer.append(", ");

    if(day < 10)
      buffer.append('0');
    buffer.append(day);
    buffer.append(' ');

    buffer.append(strMonth);
    buffer.append(' ');
    buffer.append(year);
    buffer.append(' ');

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
    buffer.append(' ');

    buffer.append(strTimeZone);
  }

  private static final String[] DAYS_OF_WEEK = {
    "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
  };
  private static final String[] MONTHS_OF_YEAR = {
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
  };

  private String getDayName(int dayOfWeek) {
    return DAYS_OF_WEEK[dayOfWeek-1];
  }

  private String getMonthName(int monthOfYear) {
    return MONTHS_OF_YEAR[monthOfYear - 1];
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
