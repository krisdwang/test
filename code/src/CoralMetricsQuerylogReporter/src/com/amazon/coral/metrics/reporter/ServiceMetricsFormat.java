package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.Group;

import javax.measure.unit.SI;
import javax.measure.unit.NonSI;
import javax.measure.unit.Unit;
import java.text.NumberFormat;
import java.io.IOException;
import java.util.Iterator;

class ServiceMetricsFormat {

  public static final String GROUP_NAME = "ServiceMetrics";
  public static final String SERVICE_NAME = "ServiceName";
  public static final String OPERATION = "Operation";

  private final NumberFormat threeSigDigits = NumberFormat.getInstance();
  private final NumberFormat sampleFormat = NumberFormat.getInstance();

  private static final Unit<?> MILLISECOND = SI.MILLI(SI.SECOND);
  private static final Unit<?> NANOSECOND = SI.NANO(SI.SECOND);

  private static final Unit<?>[] validNonTimeUnits = {NonSI.PERCENT, Unit.ONE};

  ServiceMetricsFormat() {
    // format numbers to three significant digits
    threeSigDigits.setGroupingUsed(false);
    threeSigDigits.setMaximumFractionDigits(3);

    sampleFormat.setGroupingUsed(false);
    sampleFormat.setMaximumFractionDigits(0);
  }

  void writeUnit(Appendable writer, Unit<?> unit)
    throws IOException {

    // print unit's string representation
    writer.append(unit.toString());
  }

  double toMilliseconds(double value, Unit<?> unit) {

    // Check the Unit and do a fast check for SECOND, MILLISECONDS or
    // NANOSECONDS with the standard Unit classes and convert whatever
    // it is we have into MILLISECONDS
    if(!MILLISECOND.equals(unit)) {
      if(SI.SECOND.equals(unit)) {
        value *= 1000;
      } else if(NANOSECOND.equals(unit)) {
        // nano-second = 10^-9
        // milli-second = 10^-3
        // conversion = value / 10^6
        value /= 1000000;
      } else {
        value = unit.getConverterTo(MILLISECOND).convert(value);
      }
    }

    return value;

  }

  void writeSample(Appendable writer, String name, String criticality, char typeCode, double value, Unit<?> unit, int sampleSize)
    throws IOException {

    // remap to milliseconds if the specified unit is a time unit
    if(unit.getSystemUnit().equals(SI.SECOND)) {
      // convert to milliseconds
      value = toMilliseconds(value, unit);
      unit = MILLISECOND;
    }

    writer.append(name);

    if(criticality != null && !"".equals(criticality)) {
      writer.append(':');
      writer.append(criticality);
    }

    writer.append('#');
    writer.append(typeCode);
    writer.append('/');

    writer.append(threeSigDigits.format(value));

    writer.append('/');
    writer.append(sampleFormat.format(sampleSize));
    writer.append('/');

    writeUnit(writer, unit);
  }

  public void writeLevel(Appendable writer, String name, double value, Unit<?> unit, int repeat)
    throws IOException {
    writeSample(writer, name, null, 'L', value, unit, repeat);
  }

  public void writeTime(Appendable writer, String name, double value, Unit<?> unit, int repeat)
    throws IOException {

    // timers must have SI time units
    if(!unit.getSystemUnit().equals(SI.SECOND))
      return;

    writeSample(writer, name, null, 'T', value, unit, repeat);
  }

  public void writeCount(Appendable writer, String name, double value, Unit<?> unit, int repeat)
    throws IOException {

    writeSample(writer, name, null, 'C', value, unit, repeat);
  }

  public void writeBeginLine(Appendable writer, Group group)
    throws IOException {

    writer.append(group.getName());
    writer.append('=');
  }

  public void writeEndLine(Appendable writer)
    throws IOException {

    writer.append('\n');
  }

  private void writeAttribute(Appendable writer, String value, boolean writeSeparator)
    throws IOException {

    if(writeSeparator)
      writer.append(':');
    writer.append(value);
  }

  public void writeBeginGroup(Appendable writer, Group group)
    throws IOException {

    boolean attributeWritten = false;
    String serviceName = group.getAttributeValue(SERVICE_NAME);
    String operation = group.getAttributeValue(OPERATION);

    if(serviceName != null) {
      writeAttribute(writer, serviceName, attributeWritten);
      attributeWritten = true;
    }
    if(operation != null) {
      writeAttribute(writer, operation, attributeWritten);
      attributeWritten = true;
    }

    Iterator<String> iter = group.getAttributeNames();
    while(iter.hasNext()) {
      String name = iter.next();
      if(!name.equals(SERVICE_NAME) && !name.equals(OPERATION)) {
        writeAttribute(writer, group.getAttributeValue(name), attributeWritten);
        attributeWritten = true;
      }
    }

    if(attributeWritten)
      writer.append('=');
  }

  public void writeEndGroup(Appendable writer)
    throws IOException {

    writer.append(';');
  }

  public void writeSampleSeparator(Appendable writer)
    throws IOException {

    writer.append(',');
  }
}
