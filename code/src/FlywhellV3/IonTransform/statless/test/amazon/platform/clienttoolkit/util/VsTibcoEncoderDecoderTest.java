package amazon.platform.clienttoolkit.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.*;

import junit.framework.TestCase;

import org.junit.Test;

import amazon.platform.clienttoolkit.util.VsTibcoEncoderDecoderTest;

import com.amazon.dse.messaging.datagram.IonDatagramConverter;


/**
 * The similar to HessianEncoderDecoderTest.
 * 
 * @author kaitchuc
 */
public class VsTibcoEncoderDecoderTest extends TestCase {

	private IonDatagramConverter encDec = new IonDatagramConverter();

	public static void main(String[] args) {
		//org.apache.log4j.BasicConfigurator.configure();
		VsTibcoEncoderDecoderTest encDecRun = new VsTibcoEncoderDecoderTest();
		long start = System.currentTimeMillis();
		for (int i = 0; i < 100; i++)
			try {
				encDecRun.testEncoderDecoder();
				encDecRun.testAppend();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		System.out.print((System.currentTimeMillis() - start) / 1000.0);
        MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        System.out.println("Max memory used: "+ mu.getCommitted());
	}

	@Test
	public void testEncoderDecoder() throws Exception {
		HashMap hmap = new HashMap();
		for (int i = 0; i < 100; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 100; j++)
				v.add("Hello: " + j);
			hmap.put("HashKey " + i, v);
		}
		TreeMap tmap = new TreeMap();
		for (int i = 0; i < 100; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 100; j++)
				v.add("Hello: " + j);
			tmap.put("TreeKey " + i, v);
		}
//		ByteArrayOutputStream bout = new ByteArrayOutputStream();
//		hmap.writeTo(bout);
//		byte[] hbyte = bout.toByteArray();
//		bout.reset();
//		
//		tmap.writeTo(bout);
//		byte[] tbyte = bout.toByteArray();
//		bout.reset();
		
		byte[] hbyte = encDec.serialize(hmap);
		byte[] tbyte = encDec.serialize(tmap);
	//	byte[] offbyte = new byte[hbyte.length + tbyte.length];
		
		// System.arraycopy(hbyte, 0, offbyte, 0, hbyte.length);
		// System.arraycopy(tbyte, 0, offbyte, hbyte.length, tbyte.length);
		Map HoutMap = encDec.deserialize(hbyte);
		Map ToutMap = encDec.deserialize(tbyte);
		
//		Map HoutMap = new HashMap(hbyte,null);
//		Map ToutMap = new TreeMap(tbyte,null);
		
		// Map H2outMap = encDec.deserialize(offbyte,0,hbyte.length);
		// Map T2outMap = encDec.deserialize(offbyte,hbyte.length,tbyte.length);

	//	FileOutputStream out = new FileOutputStream("Output.Hessian");
	//	out.write(tbyte);
	//	out.close();

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
				String arry = (String) iterSub1.next();
				String arryOut = (String) iterOutSub1.next();
				assertEquals(arry, arryOut);
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
				String arry = (String) iterSub1.next();
				String arryOut = (String) iterOutSub1.next();
				assertEquals(arry, arryOut);
			}
		}

		// //Test offset. There is no guarentee that entries don't shuffle
		// arround.
		// //values are the same
		// iter = hmap.entrySet ().iterator ();
		// iterOut = H2outMap.entrySet ().iterator ();
		// while (iter.hasNext ())
		// {
		// Map.Entry entry = (Map.Entry)iter.next();
		// assertTrue(H2outMap.containsKey(entry.getKey()));
		// assertTrue(hmap.containsKey(((Map.Entry)iterOut.next()).getKey()));

		// Iterator iterSub1 = ((AbstractList)entry.getValue()).iterator();
		// Iterator iterOutSub1 =
		// ((AbstractList)H2outMap.get(entry.getKey())).iterator();

		// while (iterSub1.hasNext())
		// {
		// String arry = (String)iterSub1.next();
		// String arryOut = (String) iterOutSub1.next();
		// assertEquals(arry,arryOut);
		// }
		// }

		// iter = tmap.entrySet ().iterator ();
		// iterOut = T2outMap.entrySet ().iterator ();
		// while (iter.hasNext ())
		// {
		// Map.Entry entry = (Map.Entry)iter.next();
		// assertTrue(T2outMap.containsKey(entry.getKey()));
		// assertTrue(tmap.containsKey(((Map.Entry)iterOut.next()).getKey()));

		// Iterator iterSub1 = ((AbstractList)entry.getValue()).iterator();
		// Iterator iterOutSub1 =
		// ((AbstractList)T2outMap.get(entry.getKey())).iterator();

		// while (iterSub1.hasNext())
		// {
		// String arry = (String)iterSub1.next();
		// String arryOut = (String) iterOutSub1.next();
		// assertEquals(arry,arryOut);
		// }
		// }

		assertNotSame(hmap, tmap);
		assertNotSame(HoutMap, ToutMap);
		// assertNotSame(H2outMap, T2outMap);
	}

	@Test
	public void testAppend() throws Exception {
		HashMap hmap = new HashMap();
		for (int i = 0; i < 5; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 10; j++)
				v.add("Hello: " + j);
			hmap.put("HashKey " + i, v);
		}
		byte[] hbyte = encDec.serialize(hmap);
		for (int i = 5; i < 10; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 10; j++)
				v.add("Hello: " + j);

			HashMap m = new HashMap();
			m.put("HashKey " + i, v);
			Map out = encDec.deserialize(hbyte, 1);
			out.putAll(m);
			hbyte = encDec.serialize(out);
		}
		// FileOutputStream f = new FileOutputStream("/home/kaitchuc/output");
		// f.write(hbyte);
		for (int i = 5; i < 10; i++) {
			Vector v = new Vector();
			for (int j = 0; j < 10; j++)
				v.add("Hello: " + j);
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
				String arry = (String) iterSub1.next();
				String arryOut = (String) iterOutSub1.next();
				assertEquals(arry, arryOut);
			}
		}
	}

}
