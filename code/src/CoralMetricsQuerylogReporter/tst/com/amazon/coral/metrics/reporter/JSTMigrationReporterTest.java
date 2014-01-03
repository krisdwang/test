package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;
import javax.measure.unit.Unit;
import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.locks.ReentrantLock;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;
import com.amazon.coral.metrics.NullLog;
import com.amazon.coral.metrics.*;
import java.util.TimeZone;

import static com.amazon.coral.metrics.GroupBuilder.DEFAULT_GROUP;

public class JSTMigrationReporterTest {

  @Test
  public void operationNameContainsService() {

    final CharSequence[] program = new CharSequence[1];
    final CharSequence[] operation = new CharSequence[1];

    Reporter delegate = new NullReporter() {
      @Override
      public void addProperty(Group group, String name, CharSequence value) {
        if("Program".equals(name))
          program[0] = value;

        if("Operation".equals(name))
          operation[0] = value;

        if("Service".equals(name))
          fail();
      }
    };

    JSTMigrationReporter r = new JSTMigrationReporter(delegate);

    r.beginReport();
    r.addProperty(DEFAULT_GROUP, "Program", "FooProgram");
    r.addProperty(DEFAULT_GROUP, "Service", "FooService");
    r.addProperty(DEFAULT_GROUP, "Operation", "FooOperation");
    r.endReport();

    assertEquals("FooProgram", program[0]);
    assertEquals("FooService.FooOperation", operation[0]);
  }

  @Test
  public void operationNameContainsServiceOnLastEndReport() {

    final CharSequence[] program = new CharSequence[1];
    final CharSequence[] operation = new CharSequence[1];

    Reporter delegate = new NullReporter() {
      @Override
      public void addProperty(Group group, String name, CharSequence value) {
        if("Program".equals(name))
          program[0] = value;

        if("Operation".equals(name))
          operation[0] = value;

        if("Service".equals(name))
          fail();
      }
    };

    JSTMigrationReporter r = new JSTMigrationReporter(delegate);

    r.beginReport();
    r.beginReport();
    r.addProperty(DEFAULT_GROUP, "Program", "FooProgram");
    r.addProperty(DEFAULT_GROUP, "Service", "FooService");
    r.addProperty(DEFAULT_GROUP, "Operation", "FooOperation");

    // not just yet
    r.endReport();
    assertEquals("FooProgram", program[0]);
    assertNull(operation[0]);

    r.endReport();
    assertEquals("FooProgram", program[0]);
    assertEquals("FooService.FooOperation", operation[0]);
  }

  @Test
  public void noService() {

    final CharSequence[] program = new CharSequence[1];
    final CharSequence[] operation = new CharSequence[1];

    Reporter delegate = new NullReporter() {
      @Override
      public void addProperty(Group group, String name, CharSequence value) {
        if("Program".equals(name))
          program[0] = value;

        if("Operation".equals(name))
          operation[0] = value;

        if("Service".equals(name))
          fail();
      }
    };

    JSTMigrationReporter r = new JSTMigrationReporter(delegate);

    r.beginReport();
    r.addProperty(DEFAULT_GROUP, "Program", "FooProgram");
    r.addProperty(DEFAULT_GROUP, "Operation", "FooOperation");
    r.endReport();

    assertEquals("FooProgram", program[0]);
    assertEquals("FooOperation", operation[0]);
  }

  @Test
  public void serviceWithNoOperation() {

    final CharSequence[] program = new CharSequence[1];
    final CharSequence[] operation = new CharSequence[1];

    Reporter delegate = new NullReporter() {
      @Override
      public void addProperty(Group group, String name, CharSequence value) {
        if("Program".equals(name))
          program[0] = value;

        if("Operation".equals(name))
          operation[0] = value;

        if("Service".equals(name))
          fail();
      }
    };

    JSTMigrationReporter r = new JSTMigrationReporter(delegate);

    r.beginReport();
    r.addProperty(DEFAULT_GROUP, "Program", "FooProgram");
    r.addProperty(DEFAULT_GROUP, "Service", "FooService");
    r.endReport();

    assertEquals("FooProgram", program[0]);
    assertNull(operation[0]);
  }

  @Test
  public void coralReporter() {
    final CharSequence[] program = new CharSequence[1];
    final CharSequence[] operation = new CharSequence[1];

    QuerylogFormat f = new QuerylogFormat(new StaticCalendarFactory(TimeZone.getTimeZone("UTC"))) {
      @Override
      public void writeProperty(StringBuilder sb, String name, CharSequence value) throws IOException {
        if("Program".equals(name)) {
          if(program[0] != null)
            fail();

          program[0] = value;
        }

        if("Operation".equals(name)) {
          if(program[0] != null)
            fail();

          operation[0] = value;
        }

        if("Service".equals(name))
          fail();
      }
    };

    QuerylogReporter realReporter = new QuerylogReporter(new StringWriter(), new ReentrantLock(), f);
    JSTMigrationReporter r = new JSTMigrationReporter(realReporter);

    r.beginReport();
    r.addProperty(DEFAULT_GROUP, "Operation", "FooOperation");
    r.addProperty(DEFAULT_GROUP, "Service", "FooService");
    r.addProperty(DEFAULT_GROUP, "Program", "FooProgram");
    r.endReport();

    assertEquals("FooProgram", program[0]);
    assertEquals("FooService.FooOperation", operation[0]);
  }

}
