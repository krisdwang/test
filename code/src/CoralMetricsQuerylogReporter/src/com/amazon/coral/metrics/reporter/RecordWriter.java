package com.amazon.coral.metrics.reporter;

import java.io.IOException;
import java.io.CharArrayWriter;
import java.io.Writer;

/**
 * A {@code RecordWriter} allows for record-level buffering to be added to
 * any {@code Writer}. The {@code QuerylogReporter} does one flush() per
 * record; by recordsPreviouslyBuffereding flush()s we can recordsPreviouslyBuffered the number of records being
 * written to the underlying {@code Writer}. This tool leverages that
 * information to buffer records in memory until some limit is reached
 * and will write out a batch of records all at once. This is handy in some
 * scenarios where frequent I/O is an issue.
 *
 * @author Eric Crahen <crahen@amazon.com>
 */
public class RecordWriter extends Writer {

  private final Writer writer;
  private final int recordsToBuffer;
  private CharArrayWriter buffer = new CharArrayWriter();
  private int recordsPreviouslyBuffered = 0;

  public RecordWriter(Writer writer, int recordsToBuffer) {
    if(writer == null)
      throw new IllegalArgumentException();
    if(recordsToBuffer < 0)
      throw new IllegalArgumentException();
    this.writer = writer;
    this.recordsToBuffer = recordsToBuffer;
  }

  @Override
  public void write(char[] cbuf, int off, int len)
    throws IOException {
    buffer.write(cbuf, off, len);
  }

  @Override
  public void flush()
    throws IOException {
    if(++recordsPreviouslyBuffered >= recordsToBuffer)
      doFlush();
  }

  @Override
  public void close()
    throws IOException {
    doFlush();
    writer.close();
  }

  private void doFlush()
    throws IOException {

    buffer.flush();
    buffer.close();
    writer.write( buffer.toCharArray() );
    writer.flush();
    buffer = new CharArrayWriter();
    recordsPreviouslyBuffered = 0;

  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
