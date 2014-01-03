// vim: et ts=8 sts=2 sw=2 tw=0
package com.amazon.coral.avail;

import static org.junit.Assert.*;
import org.junit.Test;

import com.amazon.coral.model.Model;
import com.amazon.coral.model.ModelIndex;
import com.amazon.coral.model.StructureModel;
import com.amazon.coral.model.reflect.DetectedModelIndexFactory;

public class AvailabilityErrorModelIndexFactoryTest {

  @Test
  public void modelIndexLoads() throws Throwable {
    DetectedModelIndexFactory factory = new DetectedModelIndexFactory();
    ModelIndex index = factory.newModelIndex();
    assertNotNull(index);
    Model model = index.getModel(new Model.Id("ThrottlingException", "com.amazon.coral.availability#"));
    assertNotNull(model);
    assertTrue(model instanceof StructureModel);
  }

}
