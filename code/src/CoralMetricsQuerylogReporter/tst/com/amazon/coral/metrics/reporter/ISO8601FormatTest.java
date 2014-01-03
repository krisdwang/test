package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

public class ISO8601FormatTest {

  private final static long now = 1205437768683L; // Using a constant since SimpleDate will round for some values
  private final static ISO8601Format idf = new ISO8601Format();

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

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    sdf.setTimeZone(timeZone);
    String expected = sdf.format(new Date(now));

    StringBuilder sb = new StringBuilder();
    idf.format(sb, now, new StaticCalendarFactory(timeZone));
    String result = sb.toString();

    assertEquals(expected, result);

  }

}
