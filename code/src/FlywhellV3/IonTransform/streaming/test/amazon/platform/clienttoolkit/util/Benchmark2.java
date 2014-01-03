package amazon.platform.clienttoolkit.util;

import java.io.*;
import java.util.*;

import com.amazon.dse.messaging.datagram.IonDatagramConverter;
import com.amazon.dse.messaging.datagramConverter.DecodingException;
import com.amazon.dse.messaging.datagramConverter.EncodingException;

/**
 * @author kaitchuc A class designed to benchmark Hessian with real production
 *         data. Pass in the names of files on the command line that contain
 *         maps that are java serialized and we can run a benchmark with them.
 */
public class Benchmark2 {

  //  private static IonDatagramConverter hed = new IonDatagramConverter();
    private static TibrvdDatagramConverter hed = new TibrvdDatagramConverter();
    
    private static int IdealSize = 0;

    private static int outer = 3;

    private static int inner = 6;

    /**
     * @param args
     *            The names of all the files you want to read in.
     * @throws IOException
     * @throws FileNotFoundException
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ArrayList<Map> maps = readInFilesContents(args);
        //        Map bigMap = new HashMap();
        //        for (int i=0;i<maps.size();i++) {
        //            bigMap.put("Key "+i,maps.get(i));
        //        }
        //        maps.clear();
        //        maps.add(bigMap);

        byte[][] encodedMaps = new byte[0][];
        ArrayList<Map> decodedMaps;
        encodedMaps = encodeAll(maps);
        decodedMaps = decodeAll(encodedMaps);
        if (!maps.equals(decodedMaps))
            throw new IllegalStateException("Decoding Failed.");

        double[] decode_times = new double[outer];
        double[] encode_times = new double[outer];
        long   start, stop, total;
        long[] times = new long[inner];

        for (int i = 0; i < outer; i++) {

            total = 0;
            for (int j = 0; j < inner; j++) {
                start = System.currentTimeMillis();
                encodedMaps = encodeAll(maps);
                stop = System.currentTimeMillis();
                times[j] = stop - start;
                total += times[j];
            }
            encode_times[i] = total;
            System.out.println("Encode Time (ms): " + encode_times[i]);

            // we really need to reset this since System.out.println can be expensive
            total = 0;
            for (int j = 0; j < inner; j++) {
                start = System.currentTimeMillis();
                decodedMaps = decodeAll(encodedMaps);
                if (!maps.equals(decodedMaps))
                    throw new IllegalStateException("Decoding Failed.");
                stop = System.currentTimeMillis();
                times[j] = stop - start;
                total += times[j];
            }
            decode_times[i] = total;
            System.out.println("Decode Time (ms): " + decode_times[i]);
        }
        int size = 0;
        for (int i = 0; i < encodedMaps.length; i++) {
            size += encodedMaps[i].length;
        }
        System.out.println("Size: " + size + " bytes");

        double ave = 0.0;
        System.out.print("Encoding: ");
        for (int ii=0; ii<outer; ii++) {
            ave += encode_times[ii];
            if (ii != 0) System.out.print(", ");
            System.out.print(encode_times[ii]);
        }
        System.out.println(" = " + ave / outer);

        ave = 0;
        System.out.print("Decoding: ");
        for (int ii=0; ii<outer; ii++) {
            ave += decode_times[ii];
            if (ii != 0) System.out.print(", ");
            System.out.print(decode_times[ii]);
        }
        System.out.println(" = " + ave / outer);
    }

    private static boolean checkEquality(ArrayList<Map> maps, ArrayList<Map> decodedMaps)
    throws UnsupportedEncodingException {
        // IdealSize = 8+2;
        if (maps.size() != decodedMaps.size())
            return false;
        for (int i = 0; i < maps.size(); i++) {
            Map a = maps.get(i);
            Map b = decodedMaps.get(i);
            if (!checkNestedEquality(a, b))
                return false;
        }
        return true;
    }

    private static boolean checkNestedEquality(Object one, Object two) throws UnsupportedEncodingException {
        if (one instanceof Map && two instanceof Map) {
            // IdealSize +=2;
            Map a = (Map) one;
            Map b = (Map) two;
            Set<Map.Entry> es = a.entrySet();
            Map.Entry current;
            for (Iterator<Map.Entry> j = es.iterator(); j.hasNext();) {
                current = j.next();
                // addSize(current.getKey());
                if (!checkNestedEquality(current.getValue(), b.get(current.getKey())))
                    return false;
            }
            return true;
        } else if (one instanceof List && two instanceof List) {
            // IdealSize +=2;
            List a = (List) one;
            List b = (List) two;
            for (int i = 0; i < a.size(); i++) {
                if (!checkNestedEquality(a.get(i), b.get(i)))
                    return false;
            }
            return true;
        } else {
            // addSize(one);
            // if (one.equals(two))
            return true;
            // return false;
        }

    }

    private static void addSize(Object one) throws UnsupportedEncodingException {
        if (one instanceof Integer)
            IdealSize += 5;
        else if (one instanceof Long || one instanceof Double)
            IdealSize += 9;
        else if (one instanceof String)
            IdealSize += ((String) one).getBytes("UTF8").length + 2;
        else if (one instanceof Boolean)
            IdealSize += 1;
        else
            System.out.println(one.getClass() + "");
    }

    private static ArrayList<Map> decodeAll(byte[][] encodedMaps) throws DecodingException, amazon.platform.clienttoolkit.util.DecodingException {
        ArrayList<Map> result = new ArrayList<Map>(encodedMaps.length);
        for (int i = 0; i < encodedMaps.length; i++) {
            Map m = hed.deserialize(encodedMaps[i]);
            result.add(m);
        }
        return result;
    }

    private static byte[][] encodeAll(ArrayList<Map> maps) throws EncodingException, amazon.platform.clienttoolkit.util.EncodingException {
        byte[][] result = new byte[maps.size()][];
        for (int i = 0; i < maps.size(); i++) {
            result[i] = hed.serialize(maps.get(i));
        }
        return result;
    }

    private static ArrayList<Map> readInFilesContents(String[] args) throws FileNotFoundException,
    IOException {
        ArrayList<Map> result = new ArrayList<Map>(args.length);

        for (int i = 0; i < args.length; i++) {
            ObjectInputStream in = new ObjectInputStream(new FileInputStream(args[i]));
            try {
                while (true)
                    result.add((Map) in.readObject());
            } catch (EOFException e) {
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            in.close();
        }
        return result;
    }
    //    static final boolean removeOutliers = true;
    //    static double evaluateSample(long[] samples, long total, long total2, int count) 
    //    {
    //        double dtot = total;
    //        double ave  = dtot / count;
    //        
    //        double dtot2 = total2;
    //        dtot = dtot * dtot;
    //        dtot = dtot / count;
    //        double stddev = (count > 1) ? (dtot2 - dtot) / (count - 1) : 0.0;
    //        stddev = Math.sqrt(stddev);
    //        
    //        long lower = (long)(ave - stddev);
    //        long upper = (long)(ave + stddev);
    //        long adj_count = 0;
    //        total = 0;
    //        for (int jj=0; jj<count; jj++) {
    //            long x = samples[jj];
    //            if (removeOutliers && (x < lower || x > upper)) continue;
    //            adj_count++;
    //            total += x;
    //        }
    //        
    //        double adjusted = total;
    //        if (adj_count > 0) {
    //            adjusted = adjusted / adj_count;
    //            //adj = Double.toString(adjusted);
    //        }
    //        else {
    //            adjusted = -1.0;
    //        }
    //
    ////adjusted = median(samples, count);
    //        return adjusted;
    //    }
    //    static double median(long[] samples, int count) {
    //        double median = -1.0;
    //        
    //        boolean anychange;
    //        int     last = count - 1;
    //        do {
    //            anychange = false;
    //            for (int ii=0; ii < last; ii++) {
    //                if (samples[ii] > samples[ii+1]) {
    //                    long temp = samples[ii];
    //                    samples[ii] = samples[ii + 1];
    //                    samples[ii + 1] = temp;
    //                    anychange = true;
    //                }
    //            }
    //            last--;
    //        } while(anychange);
    //        
    //        median = samples[count/2];
    //        if ((count & 1) == 0) {
    //            median = (median + samples[(count/2) + 1]) /2.0;
    //        }
    //        
    //        return median;
    //    }

}
