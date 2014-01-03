package com.amazon.coral.metrics.reporter;

import java.util.Calendar;

/**
 * A {@code CalendarFactory} abstracts from a {@code NumberFormat}
 * how a {@code Calendar} set to the correct time zone is obtained.
 */
interface CalendarFactory {

  Calendar newCalendar();

}
