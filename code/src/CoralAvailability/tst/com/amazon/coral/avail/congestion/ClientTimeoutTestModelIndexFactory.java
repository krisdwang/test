package com.amazon.coral.avail.congestion;

import java.io.StringReader;

import com.amazon.coral.model.xml.XmlModelIndexFactory;

/**
 * A model index factory that returns a simple XML model used by the
 * client timeout handler tests.
 */
public class ClientTimeoutTestModelIndexFactory
extends XmlModelIndexFactory {

  private final static String model =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
    "<definition assembly=\"clientTimeoutHandler\" version=\"1.0\">" +
    "  <service name=\"fooService\">" +
    "    <operation target=\"mierda\"/>" +
    "    <operation target=\"schijt\"/>" +
    "    <operation target=\"scheisse\"/>" +
    "  </service>" +
    "  <service name=\"barService\">" +
    "    <operation target=\"hovno\"/>" +
    "    <operation target=\"kuso\"/>" +
    "    <operation target=\"dahBien\"/>" +
    "  </service>" +
    "  <operation name=\"mierda\"/>" +
    "  <operation name=\"schijt\"/>" +
    "  <operation name=\"scheisse\"/>" +
    "  <operation name=\"hovno\"/>" +
    "  <operation name=\"kuso\"/>" +
    "  <operation name=\"dahBien\"/>" +
    "</definition>";

  public ClientTimeoutTestModelIndexFactory() {
    super(new StringReader(model));
  }
}
