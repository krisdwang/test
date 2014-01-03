package com.amazon.coral.metrics.reporter;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * A {@code StaticCalendarFactory} is a {@code CalendarFactory} that
 * caches a single {@code Calendar} instance for all threads.
 */
class StaticCalendarFactory implements CalendarFactory {

  private final Calendar calendar = Calendar.getInstance();

  StaticCalendarFactory(TimeZone timeZone) {
    calendar.setTimeZone(timeZone);
  }

  public Calendar newCalendar() {
    return calendar;
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
