package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.*;

public class RecordWriterTest {

  @Test(expected=IllegalArgumentException.class)
  public void nullDelegate() {
    new RecordWriter(null, 10);
  }

  @Test(expected=IllegalArgumentException.class)
  public void illegalCount() {
    new RecordWriter(new StringWriter(), -10);
  }

  // buffer no records and buffer 1 record are basically equivalent
  @Test
  public void buffer0()
    throws Throwable {
    String message = "Hello";
    StringWriter out = new StringWriter();
    Writer w = new RecordWriter(out, 0);
    w.write(message);
    w.flush();
    assertEquals(out.toString(), message);
    w.write(message);
    w.flush();
    assertEquals(out.toString(), message+message);
  }

  @Test
  public void buffer1()
    throws Throwable {
    String message = "Hello";
    StringWriter out = new StringWriter();
    Writer w = new RecordWriter(out, 1);
    w.write(message);
    w.flush();
    assertEquals(out.toString(), message);
    w.write(message);
    w.flush();
    assertEquals(out.toString(), message+message);
  }

  @Test
  public void buffer2()
    throws Throwable {
    String message = "Hello";
    StringWriter out = new StringWriter();
    Writer w = new RecordWriter(out, 2);
    w.write(message);
    w.flush();
    assertEquals(out.toString(), "");
    w.write(message);
    w.flush();
    assertEquals(out.toString(), message+message);
  }

}
