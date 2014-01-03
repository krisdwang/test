/**
 * Derived from caucho's hessian protocall, extensively modivied for amazon's internal use.
 * @author kaitchuc
 */
package amazon.platform.clienttoolkit.util;

import java.util.*;

import junit.framework.TestCase;

import org.junit.Test;

import amazon.platform.clienttoolkit.util.HessianEncoderDecoderTest;

import com.amazon.dse.messaging.datagram.IonDatagramConverter;

/**
 * @author kaitchuc
 */
public class HessianEncoderDecoderTest extends TestCase {

	private IonDatagramConverter encDec = new IonDatagramConverter();

	/**
	 * Run through the tests repeatedly for benchmarking.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		org.apache.log4j.BasicConfigurator.configure();
		for (int i = 0; i < 10; i++)
			try {
				HessianEncoderDecoderTest encDecRun = new HessianEncoderDecoderTest();
				encDecRun.testEncoderDecoder();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	/**
	 * Test encoding and decoding nested maps, arrays, and vectors.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testEncoderDecoder() throws Exception {
		HashMap<String, Vector> hmap = new HashMap();
		for (int i = 0; i < 10; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 10; j++)
				v.add(new short[] { 0xff, 0x00, 0x01, (short) 0xFFFF,
						(short) (0xff + j * i), (short) (0x00 + j * i),
						(short) (0x01 + j * i), (short) (0xFFFF + j * i),
						(short) (0xff - j * i), (short) (0x00 - j * i),
						(short) (0x01 - j * i), (short) (0xFFFF - j * i) });
			hmap.put("HashKey " + i, v);
		}
		TreeMap<String, Vector> tmap = new TreeMap();
		for (int i = 0; i < 10; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 10; j++)
				v.add(new long[] { 0xff, 0x00, 0x01, (long) 0xFFFF,
						(long) (0xff + j * i), (long) (0x00 + j * i),
						(long) (0x01 + j * i), (long) (0xFFFF + j * i),
						(long) (0xff - j * i), (long) (0x00 - j * i),
						(long) (0x01 - j * i), (long) (0xFFFF - j * i) });
			tmap.put("TreeKey " + i, v);
		}
		byte[] hbyte = encDec.serialize(hmap);
		byte[] tbyte = encDec.serialize(tmap);
		byte[] offbyte = new byte[hbyte.length + tbyte.length];
		System.arraycopy(hbyte, 0, offbyte, 0, hbyte.length);
		System.arraycopy(tbyte, 0, offbyte, hbyte.length, tbyte.length);
		Map HoutMap = encDec.deserialize(hbyte);
		Map ToutMap = encDec.deserialize(tbyte);
//		Map H2outMap = encDec.deserialize(offbyte, 0, hbyte.length);
//		Map T2outMap = encDec.deserialize(offbyte, hbyte.length, tbyte.length);

		// FileOutputStream out = new FileOutputStream("Output.Hessian");
		// out.write(tbyte);
		// out.close();

		// Keys are the same. There is no guarentee that entries don't shuffle
		// arround.
		Iterator iter = hmap.entrySet().iterator();
		Iterator iterOut = HoutMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			assertTrue(HoutMap.containsKey(entry.getKey()));
			assertTrue(hmap.containsKey(((Map.Entry) iterOut.next()).getKey()));

			Iterator iterSub1 = ((Collection) entry.getValue()).iterator();
			Iterator iterOutSub1 = ((Collection) HoutMap.get(entry.getKey()))
					.iterator();

			while (iterSub1.hasNext()) {
				short[] arry = (short[]) iterSub1.next();
				short[] arryOut = (short[]) iterOutSub1.next();
				for (int i = 0; i < arry.length; i++)
					assertEquals(arry[i], arryOut[i]);
			}
		}

		iter = tmap.entrySet().iterator();
		iterOut = ToutMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			assertTrue(ToutMap.containsKey(entry.getKey()));
			assertTrue(tmap.containsKey(((Map.Entry) iterOut.next()).getKey()));

			Iterator iterSub1 = ((Collection) entry.getValue()).iterator();
			Iterator iterOutSub1 = ((Collection) ToutMap.get(entry.getKey()))
					.iterator();

			while (iterSub1.hasNext()) {
				long[] arry = (long[]) iterSub1.next();
				long[] arryOut = (long[]) iterOutSub1.next();
				for (int i = 0; i < arry.length; i++)
					assertEquals(arry[i], arryOut[i]);
			}
		}

//		// Test offset. There is no guarentee that entries don't shuffle
//		// arround.
//		// values are the same
//		iter = hmap.entrySet().iterator();
//		iterOut = H2outMap.entrySet().iterator();
//		while (iter.hasNext()) {
//			Map.Entry entry = (Map.Entry) iter.next();
//			assertTrue(H2outMap.containsKey(entry.getKey()));
//			assertTrue(hmap.containsKey(((Map.Entry) iterOut.next()).getKey()));
//
//			Iterator iterSub1 = ((Collection) entry.getValue()).iterator();
//			Iterator iterOutSub1 = ((Collection) H2outMap.get(entry.getKey()))
//					.iterator();
//
//			while (iterSub1.hasNext()) {
//				short[] arry = (short[]) iterSub1.next();
//				short[] arryOut = (short[]) iterOutSub1.next();
//				for (int i = 0; i < arry.length; i++)
//					assertEquals(arry[i], arryOut[i]);
//			}
//		}
//
//		iter = tmap.entrySet().iterator();
//		iterOut = T2outMap.entrySet().iterator();
//		while (iter.hasNext()) {
//			Map.Entry entry = (Map.Entry) iter.next();
//			assertTrue(T2outMap.containsKey(entry.getKey()));
//			assertTrue(tmap.containsKey(((Map.Entry) iterOut.next()).getKey()));
//
//			Iterator iterSub1 = ((Collection) entry.getValue()).iterator();
//			Iterator iterOutSub1 = ((Collection) T2outMap.get(entry.getKey()))
//					.iterator();
//
//			while (iterSub1.hasNext()) {
//				long[] arry = (long[]) iterSub1.next();
//				long[] arryOut = (long[]) iterOutSub1.next();
//				for (int i = 0; i < arry.length; i++)
//					assertEquals(arry[i], arryOut[i]);
//			}
//		}

		assertNotSame(hmap, tmap);
		assertNotSame(HoutMap, ToutMap);
//		assertNotSame(H2outMap, T2outMap);
	}

	/**
	 * Test appending onto an existing encoded message.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testAppend() throws Exception {
		HashMap<String, Vector> hmap = new HashMap();
		for (int i = 0; i < 5; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 10; j++)
				v.add(new short[] { 0xff, 0x00, 0x01, (short) 0xFFFF,
						(short) (0xff + j * i), (short) (0x00 + j * i),
						(short) (0x01 + j * i), (short) (0xFFFF + j * i),
						(short) (0xff - j * i), (short) (0x00 - j * i),
						(short) (0x01 - j * i), (short) (0xFFFF - j * i) });
			hmap.put("HashKey " + i, v);
		}
		byte[] hbyte = encDec.serialize(hmap);
		for (int i = 5; i < 10; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 10; j++)
				v.add(new short[] { 0xff, 0x00, 0x01, (short) 0xFFFF,
						(short) (0xff + j * i), (short) (0x00 + j * i),
						(short) (0x01 + j * i), (short) (0xFFFF + j * i),
						(short) (0xff - j * i), (short) (0x00 - j * i),
						(short) (0x01 - j * i), (short) (0xFFFF - j * i) });

			HashMap m = new HashMap();
			m.put("HashKey " + i, v);
			Map out = encDec.deserialize(hbyte, 1);
			out.putAll(m);
			hbyte = encDec.serialize(out);
		}

		for (int i = 5; i < 10; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 10; j++)
				v.add(new short[] { 0xff, 0x00, 0x01, (short) 0xFFFF,
						(short) (0xff + j * i), (short) (0x00 + j * i),
						(short) (0x01 + j * i), (short) (0xFFFF + j * i),
						(short) (0xff - j * i), (short) (0x00 - j * i),
						(short) (0x01 - j * i), (short) (0xFFFF - j * i) });
			hmap.put("HashKey " + i, v);
		}
		Map HoutMap = encDec.deserialize(hbyte);

		// Keys are the same. There is no guarentee that entries don't shuffle
		// arround.
		Iterator iter = hmap.entrySet().iterator();
		Iterator iterOut = HoutMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			assertTrue(HoutMap.containsKey(entry.getKey()));
			assertTrue(hmap.containsKey(((Map.Entry) iterOut.next()).getKey()));

			Iterator iterSub1 = ((Collection) entry.getValue()).iterator();
			Iterator iterOutSub1 = ((Collection) HoutMap.get(entry.getKey()))
					.iterator();

			while (iterSub1.hasNext()) {
				short[] arry = (short[]) iterSub1.next();
				short[] arryOut = (short[]) iterOutSub1.next();
				for (int i = 0; i < arry.length; i++)
					assertEquals(arry[i], arryOut[i]);
			}
		}
	}

}
