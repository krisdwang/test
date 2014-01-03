package com.amazon.coral.metrics.reporter;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * A {@code ThreadLocalCalendarFactory} is a {@code CalendarFactory} that
 * caches a single {@code Calendar} instance per thread.
 */
class ThreadLocalCalendarFactory implements CalendarFactory {

  private final TimeZone timeZone;

  private final ThreadLocal<Calendar> protectedCalendar = new ThreadLocal<Calendar>() {
    @Override
    public Calendar initialValue() {
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeZone(timeZone);
      return calendar;
    }
  };

  ThreadLocalCalendarFactory(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public Calendar newCalendar() {
    return protectedCalendar.get();
  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
