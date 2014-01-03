/**
 * Derived from caucho's hessian protocall, extensively modivied for amazon's internal use.
 * @author kaitchuc
 */

package amazon.platform.clienttoolkit.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import amazon.platform.clienttoolkit.util.Benchmark;
import amazon.platform.clienttoolkit.util.HessianEncoderDecoderTest;
import amazon.platform.clienttoolkit.util.VsTibcoEncoderDecoderTest;

import com.amazon.dse.messaging.datagram.IonDatagramConverter;
import com.amazon.dse.messaging.datagramConverter.DecodingException;
import com.amazon.dse.messaging.datagramConverter.EncodingException;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.SystemFactory;

// import org.apache.log4j.Logger;

import junit.framework.TestCase;

/**
 * Benchmakrs HessianInput and HessianOutput so it can be profiled.
 * 
 * @author kaitchuc
 * 
 */
public class Benchmark extends TestCase {
	// private static final Logger logger =
	// Logger.getLogger(HessianEncoderDecoder.class);
    static IonDatagramConverter conv = new IonDatagramConverter();

	/**
	 * @param args
	 * @throws DecodingException
	 * @throws EncodingException
	 * @throws IOException
	 * @throws EncodingException 
	 * @throws DecodingException 
	 */
	public static void main(String[] args) throws IOException, DecodingException, EncodingException {
		long tnow = System.currentTimeMillis();
		Benchmark bench = new Benchmark();
		for (int i = 0; i < 10000; i++)
			bench.testBenchmark();
		System.out.println((System.currentTimeMillis() - tnow) / 1000.0);
		//junit.textui.TestRunner.run(HessianInputTest.class);
		//junit.textui.TestRunner.run(HessianOutputTest.class);
		junit.textui.TestRunner.run(HessianEncoderDecoderTest.class);
		junit.textui.TestRunner.run(VsTibcoEncoderDecoderTest.class);
		String str = makeRandomString(1024 * 1024);
		System.out.println("String is " + str.length() + " Characters long.");
		stringSpeed(str);
		System.gc();
		byteSpeed(randBytes(1024 * 1024));
	}

	private static byte[] randBytes(int size) {
		Random r = new Random(System.currentTimeMillis());
		byte[] randBytes = new byte[size];
		r.nextBytes(randBytes);
		return randBytes;
	}

	private static String makeRandomString(int size)
			throws UnsupportedEncodingException {
		return new String(randBytes(size), "BIG5");
	}

	private static void byteSpeed(byte[] start) throws DecodingException, EncodingException {
		long tnow = System.currentTimeMillis();

		HashMap m = new HashMap();
		m.put("bytes", start);

		byte[] bout = conv.serialize(m);
		Map mess = conv.deserialize(bout);
		byte[] done = (byte[]) mess.get("bytes");

		System.out.println((System.currentTimeMillis() - tnow) / 1000.0);
		assert done.length == start.length;
		for (int i = 0; i < start.length; i++) {
			assertEquals(start[i], done[i]);
		}
	}

	private static void stringSpeed(String str) throws DecodingException, EncodingException {
		long tnow = System.currentTimeMillis();

		HashMap m = new HashMap();
		m.put("string", str);
        byte[] bout = conv.serialize(m);
        Map mess = conv.deserialize(bout);
		String done = (String) mess.get("string");

		System.out.println((System.currentTimeMillis() - tnow) / 1000.0);
		assert done.equals(str);
	}

	/**
	 * Runs a single round of a test.
	 * @throws EncodingException 
	 * @throws DecodingException 
	 */
	public void testBenchmark() throws IOException, EncodingException, DecodingException {
		Map m = new HashMap();
		String s = "hello";

		m.put("This is a test String.", true);
//		byte[] b = new byte[] { 1 };
//		m.put("nested", b);
		m.put(s, true);
		m.put("true", true);
		m.put("false", false);
		m.put("Something", null);
		// out.writePlaceholder();
		m.put("12242.124598129", 12242.124598129);
		m.put("long",(long) 9326);
		m.put("12", -12);
		m.put("Almost Done.", true);
//		m.put(new String[][][] { { { s } } }, true);
		m.put("Date",new Date(-10000));

		HashMap<String, String> hmap = new HashMap();
		for (int i = 0; i < 10; i++) {
			hmap.put("Key " + i, "value " + i);
		}
		TreeMap<String, String> tmap = new TreeMap();
		for (int i = 0; i < 10; i++) {
			tmap.put("Key " + i, "value " + i);
		}
		m.put("hmap", hmap);
		m.put("tmap", tmap);

		byte[] bytes = conv.serialize(m);

//		FileOutputStream fileout = new FileOutputStream("Output.Hessian");
//		fileout.write(bytes);
//		fileout.close();
//		int length = bytes.length;
//		System.out.println(length);
//		m.put("bytes", bytes);
		
		Map m2 = conv.deserialize(bytes);
		assertEquals(m, m2);

		assertTrue(m2.containsKey("This is a test String."));// header
//		Object b21 = m2.get("nested");
//		byte[] b2 = (byte[]) m2.get("nested");
//		assertEquals(b[0], b2[0]);
		assertTrue(m2.containsKey(s));
		assertEquals(m2.get("true"),true);// true
		assertEquals(m2.get("false"),false);// false
		assertTrue(m2.containsKey("Something"));
		assertNull(m2.get("Something"));
		// assertEquals(m2.get(in.readObject());//placeholder
		assertEquals(m2.get("12242.124598129"),12242.124598129);// double
		assertEquals(m2.get("long"),(long) 9326);// long
		assertEquals(m2.get("12"),-12);// int
		assertEquals(m2.get("Almost Done."),true);// string
		assertEquals(m2.get(s),true);
		assertEquals(m2.get("Date"),new Date(-10000));
		Map hmapout = (Map) m2.get("hmap");
		assertTrue(hmapout.equals(hmap));// hmap
		Map tmapout = (Map) m2.get("tmap");
		assertTrue(tmapout.equals(tmap));// tmap

//		byte[] outBytes = (byte[]) m.get("bytes");
//		for (int i = 0; i < outBytes.length; i++) {
//			assertEquals(bytes[i], outBytes[i]);
//		}
//		assertEquals(length, outBytes.length);
	}

}
