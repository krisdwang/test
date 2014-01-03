package com.amazon.coral.metrics.reporter;

interface DateFormat {

  /**
   */
  void format(StringBuilder buffer, double millisSinceEpoch, CalendarFactory calendarFactory);

}
