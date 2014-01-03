package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class EndTimeFormatTest {

  private final static long now = 1205437768683L; // Using a constant since SimpleDate will round for some values
  private final static EndTimeFormat etf = new EndTimeFormat();

  @Test
  public void equivalenceUTC() {
    equivalence(TimeZone.getTimeZone("UTC"));
  }

  @Test
  public void equivalenceEST() {
    equivalence(TimeZone.getTimeZone("EST"));
  }

  @Test
  public void equivalencePDT() {
    equivalence(TimeZone.getTimeZone("PDT"));
  }

  // Confirm this is a drop in replacement for the SimpleDateFormat
  public void equivalence(TimeZone timeZone) {

    SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
    sdf.setTimeZone(timeZone);
    String expected = sdf.format(new Date(now));

    StringBuilder sb = new StringBuilder();
    etf.format(sb, now, new StaticCalendarFactory(timeZone));
    String result = sb.toString();

    assertEquals(expected, result);

  }

}
