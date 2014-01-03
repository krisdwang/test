package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.*;
import java.util.TimeZone;

public class JSTMigrationReporterFactoryTest {

  @Test(expected=IllegalArgumentException.class)
  public void nullParameter() throws Throwable {
    new JSTMigrationReporterFactory(null);
  }

}
