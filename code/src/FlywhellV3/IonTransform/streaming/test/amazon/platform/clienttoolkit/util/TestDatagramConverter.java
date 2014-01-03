/**
 *      Copyright (c) 2003 Amazon.com Inc. All Rights Reserved.
 *      AMAZON.COM CONFIDENTIAL
 *
 * Project:     Plogs
 *
 * Revision History:
 *
 *      Name            Date            Description
 *      ----            ----            -----------
 *      dons         Oct 25, 2004         Initial version
 *
 */
package amazon.platform.clienttoolkit.util;

import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.*;

import com.amazon.dse.messaging.datagram.IonDatagramConverter;
import com.amazon.dse.messaging.datagramConverter.DatagramConverter;

/**
 * TestDatagramConverter Borrowed from bsf.
 */
public class TestDatagramConverter extends TestCase {

    static byte[] datagramAsBsf = new byte[] { -32, 1, 0, -22, -18, -51, -127, -126, -34, -55, -121, -34,
            -58, -118, -122, 115, 116, 114, 105, 110, 103, -117, -114, -112, 106, 97, 118, 97, 46, 108, 97,
            110, 103, 46, 83, 116, 114, 105, 110, 103, -116, -122, 110, 117, 109, 98, 101, 114, -115, -124,
            108, 111, 110, 103, -114, -124, 98, 111, 111, 108, -113, -124, 100, 97, 116, 101, -112, -114,
            -114, 106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 68, 97, 116, 101, -34, -96, -118, -23, -127,
            -117, -122, 115, 116, 114, 105, 110, 103, -116, -28, -127, -115, 33, 10, -114, 17, -113, -21,
            -127, -112, 104, -64, -128, 1, 0, 113, 104, -108, -72 };

    static Map datagramAsMap = new HashMap(10);
    static {
        datagramAsMap.put("string", "string");
        datagramAsMap.put("number", new Long(10));
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT-8:00"));
        calendar.clear(); // calling clear() sets the milliseconds to zero
        calendar.set(2004, 10, 25, 12, 25, 7);
        datagramAsMap.put("date", calendar.getTime());
        datagramAsMap.put("bool", new Boolean(true));
    }

    public void testBSFSerialize() throws Exception {
        DatagramConverter conv = new IonDatagramConverter();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        conv.serialize(out, datagramAsMap);
        byte[] result = out.toByteArray();

        assertEquals("Array size mismatch", datagramAsBsf.length, result.length);
        for (int i = 0, n = datagramAsBsf.length; i < n; i++) {
            assertEquals("Byte array mismatch at position " + i, datagramAsBsf[i], result[i]);
        }
    }

    public void testBSFDeserialize() throws Exception {
        DatagramConverter conv = new IonDatagramConverter();
        ByteArrayInputStream in = new ByteArrayInputStream(datagramAsBsf);
        Map result = conv.deserialize(in);
        for (Iterator iter = datagramAsMap.keySet().iterator(); iter.hasNext();) {
            String key = (String) iter.next();
            Object obj1 = datagramAsMap.get(key);
            Object obj2 = result.get(key);
            assertNotNull("Null value for key '" + key + "'", obj2);
            assertEquals(obj1, obj2);
        }
    }

    public void testBSFRoundTrip() throws Exception {
        DatagramConverter conv = new IonDatagramConverter();
        ByteArrayInputStream in = new ByteArrayInputStream(datagramAsBsf);
        Map bsfd = conv.deserialize(in);
        ByteArrayOutputStream outstream = new ByteArrayOutputStream();
        conv.serialize(outstream, bsfd);
        byte[] result = outstream.toByteArray();
        assertTrue(Arrays.equals(datagramAsBsf, result));
    }

}
