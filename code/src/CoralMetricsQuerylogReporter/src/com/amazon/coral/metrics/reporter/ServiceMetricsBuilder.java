package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.Group;
import javax.measure.unit.Unit;
import java.io.IOException;
import java.io.Writer;

class ServiceMetricsBuilder {

  private final static ServiceMetricsFormat format = new ServiceMetricsFormat();

  private final StringBuilder stringBuilder = new StringBuilder();

  private Group previousGroup = null;

  public ServiceMetricsBuilder() throws IOException {

  }

//   public void addProperty(Group group, String name, CharSequence value) {
//     // service metrics does not support properties
//   }

//   public void addDate(Group group, String name, double value) {
//     // service metrics does not support dates
//   }

  public void addCount(Group group, String name, double value, Unit<?> unit, int repeat)
    throws IOException {

    applyGroup(group);
    format.writeCount(stringBuilder, name, value, unit, repeat);
  }

  public void addTime(Group group, String name, double value, Unit<?> unit, int repeat)
    throws IOException {

    applyGroup(group);
    format.writeTime(stringBuilder, name, value, unit, repeat);
  }

  public void addLevel(Group group, String name, double value, Unit<?> unit, int repeat)
    throws IOException {

    applyGroup(group);
    format.writeLevel(stringBuilder, name, value, unit, repeat);
  }

  public void appendTo(Writer w) throws IOException {
    w.append(stringBuilder);
    format.writeEndGroup(w);
    format.writeEndLine(w);
  }

  private void applyGroup(Group group) throws IOException {
    if(group.equals(previousGroup)) {
      format.writeSampleSeparator(stringBuilder);
    } else {
      if(previousGroup != null) {
        format.writeEndGroup(stringBuilder);
      }

      // determine whether it's another group within the same "line"
      // i.e. is  the group name the same, and just different attributes?
      if(previousGroup == null || !previousGroup.getName().equals(group.getName())) {
        if(previousGroup != null) {
          format.writeEndLine(stringBuilder);
        }
        format.writeBeginLine(stringBuilder, group);
      }

      format.writeBeginGroup(stringBuilder, group);

      previousGroup = group;
    }
  }
}
