package com.amazon.dse.messaging.datagram;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;

/**
 * Serializing an object for known object types.
 */
class Serializers {

    private static byte[] getRandomBytes(Random rand, int length) {
        byte[] randBytes = new byte[length];
        rand.nextBytes(randBytes);
        return randBytes;
    }
    
    static class ByteSerializer extends AbstractSerializer<Byte> {
        static final String CANONICAL_NAME = byte.class.getCanonicalName();

        @Override
        public void writeValue(Byte value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeInt(value);
        }
        @Override
        Byte getOrigionalValue(IonReader stream) {
            return (byte) stream.intValue();
        }
        @Override
        Byte generateRandomValue(Random rand, int length, int depth) {
            return new Byte((byte)rand.nextInt()); 
        }
    }

    static class ShortSerializer extends AbstractSerializer<Short> {
        static final String CANONICAL_NAME = short.class.getCanonicalName();

        @Override
        public void writeValue(Short value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeInt(value);
        }

        @Override
        Short getOrigionalValue(IonReader stream) {
            return (short) stream.intValue();
        }
        @Override
        Short generateRandomValue(Random rand, int length, int depth) {
            return new Short((short)rand.nextInt()); 
        }
    }

    static class IntSerializer extends AbstractSerializer<Integer> {
        static final String CANONICAL_NAME = int.class.getCanonicalName();

        @Override
        public void writeValue(Integer value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeInt(value);
        }

        @Override
        Integer getOrigionalValue(IonReader stream) {
            return stream.intValue();
        }
        @Override
        Integer generateRandomValue(Random rand, int length, int depth) {
            return new Integer(rand.nextInt()); 
        }
    }

    static class LongSerializer extends AbstractSerializer<Long> {
        static final String CANONICAL_NAME = long.class.getCanonicalName();

        @Override
        public void writeValue(Long value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeInt(value);
        }

        @Override
        Long getOrigionalValue(IonReader stream) {
            return stream.longValue();
        }
        @Override
        Long generateRandomValue(Random rand, int length, int depth) {
            return new Long(rand.nextLong()); 
        }
    }

    static class BigIntSerializer extends AbstractSerializer<BigInteger> {
        static final String CANONICAL_NAME = BigInteger.class.getCanonicalName();

        @Override
        public void writeValue(BigInteger value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeDecimal(new BigDecimal(value));
        }

        @Override
        BigInteger getOrigionalValue(IonReader stream) {
            throw new IllegalStateException("This should be handled by BigDecimalSerializer.");
        }
        @Override
        BigInteger generateRandomValue(Random rand, int length, int depth) {
            return new BigInteger(getRandomBytes(rand, length+1)); 
        }
    }
    
    static class BigDecimalSerializer extends AbstractSerializer<Number> {

        @Override
        public void writeValue(Number value, IonWriter writer) throws IOException {
            writer.writeDecimal((BigDecimal)value);
        }

        @Override
        Number getOrigionalValue(IonReader stream) {
            String[] annotations = stream.getAnnotations();
            if (annotations == null || annotations.length <= 0 || 
                    !BigInteger.class.getCanonicalName().equals(annotations[0]))
                return stream.bigDecimalValue();
            else
                return stream.bigDecimalValue().toBigInteger();
        }
        @Override
        BigDecimal generateRandomValue(Random rand, int length, int depth) {
            BigInteger num = new BigInteger(getRandomBytes(rand, length+1));
            BigDecimal result = new BigDecimal(num);
            return result;
        }
    }
    
    static class DoubleSerializer extends AbstractSerializer<Double> {
        static final String CANONICAL_NAME = double.class.getCanonicalName();

        @Override
        public void writeValue(Double value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeFloat(value);
        }

        @Override
        Double getOrigionalValue(IonReader stream) {
            return stream.doubleValue();
        }
        @Override
        Double generateRandomValue(Random rand, int length, int depth) {
            return Double.longBitsToDouble(rand.nextLong()); 
        }
    }

    static class FloatSerializer extends AbstractSerializer<Float> {
        static final String CANONICAL_NAME = float.class.getCanonicalName();

        @Override
        public void writeValue(Float value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeFloat(value);
        }

        @Override
        Float getOrigionalValue(IonReader stream) {
            return (float) stream.doubleValue();
        }
        @Override
        Float generateRandomValue(Random rand, int length, int depth) {
            return Float.intBitsToFloat(rand.nextInt()); 
        }
    }

    static class CharSerializer extends AbstractSerializer<Character> {
        static final String CANONICAL_NAME = char.class.getCanonicalName();

        @Override
        public void writeValue(Character value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeString(value.toString());
        }

        @Override
        public Character getOrigionalValue(IonReader stream) {
            throw new IllegalStateException("This should be handled by StringSerializer.");
        }
        @Override
        Character generateRandomValue(Random rand, int length, int depth) {
            return new Character((char)rand.nextInt()); 
        }
    }

    static class DateSerializer extends AbstractSerializer<Date> {
        @Override
        public void writeValue(Date value, IonWriter writer) throws IOException {
            writer.writeTimestampUTC(value);
        }

        @Override
        public Date getOrigionalValue(IonReader stream) {
            return stream.dateValue();
        }
        @Override
        Date generateRandomValue(Random rand, int length, int depth) {
            return new Date(rand.nextLong()); 
        }
    }

    static class BooleanSerializer extends AbstractSerializer<Boolean> {
        @Override
        public void writeValue(Boolean value, IonWriter writer) throws IOException {
            writer.writeBool(value);
        }

        @Override
        Boolean getOrigionalValue(IonReader stream) {
            return stream.booleanValue();
        }
        @Override
        Boolean generateRandomValue(Random rand, int length, int depth) {
            return rand.nextBoolean();
        }
    }

    static class ByteArraySerializer extends AbstractSerializer<byte[]> {
        @Override
        public void writeValue(byte[] value, IonWriter writer) throws IOException {
            writer.writeBlob(value);
        }

        @Override
        public byte[] getOrigionalValue(IonReader stream) {
            return stream.newBytes();
        }
        @Override
        boolean areEqual(byte[] a, byte[] b) {
            return a==null?b==null:Arrays.equals(a, b);
        }

        @Override
        byte[] generateRandomValue(Random rand, int length, int depth) {
            return getRandomBytes(rand, length);
        }
    }

    static class StringSerializer extends AbstractSerializer<Object> {
        @Override
        public void writeValue(Object value, IonWriter writer) throws IOException {
            writer.writeString(value.toString());
        }

        @Override
        public Object getOrigionalValue(IonReader stream) throws IOException {
            String[] annotations = stream.getAnnotations();
            if (annotations == null || annotations.length <= 0)
                return stream.stringValue();
            else if (char[].class.getCanonicalName().equals(annotations[0]))
                return stream.stringValue().toCharArray();
            else if (char.class.getCanonicalName().equals(annotations[0]))
                return stream.stringValue().charAt(0);
            throw new IOException("Corrupt datagram.");
        }

        @Override
        Object generateRandomValue(Random rand, int length, int depth) {
            try {
                return new String(getRandomBytes(rand, length),"UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static class CharArraySerializer extends AbstractSerializer<char[]> {
        static final String CANONICAL_NAME = char[].class.getCanonicalName();

        @Override
        public void writeValue(char[] value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeString(String.valueOf(value));
        }

        @Override
        public char[] getOrigionalValue(IonReader stream) {
            throw new IllegalStateException("This should be handled by StringSerializer.");
        }
        @Override
        boolean areEqual(char[] a, char[] b) {
            return a==null?b==null:Arrays.equals(a, b);
        }
        @Override
        char[] generateRandomValue(Random rand, int length, int depth) {
            try {
                return new String(getRandomBytes(rand, length),"UTF-8").toCharArray();
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static class ShortArrayWriter extends AbstractSerializer<short[]> {
        static final String CANONICAL_NAME = short[].class.getCanonicalName();

        @Override
        public void writeValue(short[] value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeIntList(value);
        }
    
        @Override
        short[] getOrigionalValue(IonReader stream) {
            short[] result = new short[stream.getContainerSize()];
            stream.stepInto();
            for (int i = 0; i < result.length && stream.hasNext(); i++) {
                stream.next();
                result[i] = (short) stream.intValue();
            }
            stream.stepOut();
            return result;
        }
        @Override
        boolean areEqual(short[] a, short[] b) {
            return a==null?b==null:Arrays.equals(a, b);
        }
        @Override
        short[] generateRandomValue(Random rand, int length, int depth) {
            short[] result = new short[length];
            for (int i=0;i<length;i++) {
                result[i] = (short) rand.nextInt();
            }
            return result;
        }
    }

    static class IntArrayWriter extends AbstractSerializer<int[]> {
        static final String CANONICAL_NAME = int[].class.getCanonicalName();

        @Override
        public void writeValue(int[] value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeIntList(value);
        }
    
        @Override
        int[] getOrigionalValue(IonReader stream) {
            int[] result = new int[stream.getContainerSize()];
            stream.stepInto();
            for (int i = 0; i < result.length && stream.hasNext(); i++) {
                stream.next();
                result[i] = stream.intValue();
            }
            stream.stepOut();
            return result;
        }
        @Override
        boolean areEqual(int[] a, int[] b) {
            return a==null?b==null:Arrays.equals(a, b);
        }
        @Override
        int[] generateRandomValue(Random rand, int length, int depth) {
            int[] result = new int[length];
            for (int i=0;i<length;i++) {
                result[i] = rand.nextInt();
            }
            return result;
        }
    }

    static class LongArrayWriter extends AbstractSerializer<long[]> {
        static final String CANONICAL_NAME = long[].class.getCanonicalName();

        @Override
        public void writeValue(long[] value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeIntList(value);
        }
    
        @Override
        long[] getOrigionalValue(IonReader stream) {
            long[] result = new long[stream.getContainerSize()];
            stream.stepInto();
            for (int i = 0; i < result.length && stream.hasNext(); i++) {
                stream.next();
                result[i] = stream.longValue();
            }
            stream.stepOut();
            return result;
        }
        @Override
        boolean areEqual(long[] a, long[] b) {
            return a==null?b==null:Arrays.equals(a, b);
        }
        @Override
        long[] generateRandomValue(Random rand, int length, int depth) {
            long[] result = new long[length];
            for (int i=0;i<length;i++) {
                result[i] = rand.nextLong();
            }
            return result;
        }
    }

    static class DoubleArrayWriter extends AbstractSerializer<double[]> {
        static final String CANONICAL_NAME = double[].class.getCanonicalName();

        @Override
        public void writeValue(double[] value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeFloatList(value);
        }
    
        @Override
        double[] getOrigionalValue(IonReader stream) {
            double[] result = new double[stream.getContainerSize()];
            stream.stepInto();
            for (int i = 0; i < result.length && stream.hasNext(); i++) {
                stream.next();
                result[i] = stream.doubleValue();
            }
            stream.stepOut();
            return result;
        }
        @Override
        boolean areEqual(double[] a, double[] b) {
            return a==null?b==null:Arrays.equals(a, b);
        }
        @Override
        double[] generateRandomValue(Random rand, int length, int depth) {
            double[] result = new double[length];
            for (int i=0;i<length;i++) {
                result[i] = Double.longBitsToDouble(rand.nextLong());
            }
            return result;
        }
    }

    static class FloatArrayWriter extends AbstractSerializer<float[]> {
        static final String CANONICAL_NAME = float[].class.getCanonicalName();

        @Override
        public void writeValue(float[] value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeFloatList(value);
        }
    
        @Override
        float[] getOrigionalValue(IonReader stream) {
            float[] result = new float[stream.getContainerSize()];
            stream.stepInto();
            for (int i = 0; i < result.length && stream.hasNext(); i++) {
                stream.next();
                result[i] = (float) stream.doubleValue();
            }
            stream.stepOut();
            return result;
        }
        @Override
        boolean areEqual(float[] a, float[] b) {
            return a==null?b==null:Arrays.equals(a, b);
        }
        @Override
        float[] generateRandomValue(Random rand, int length, int depth) {
            float[] result = new float[length];
            for (int i=0;i<length;i++) {
                result[i] = Float.intBitsToFloat(rand.nextInt());
            }
            return result;
        }
    }

    static class BooleanArrayWriter extends AbstractSerializer<boolean[]> {
        static final String CANONICAL_NAME = boolean[].class.getCanonicalName();

        @Override
        public void writeValue(boolean[] value, IonWriter writer) throws IOException {
            writer.addAnnotation(CANONICAL_NAME);
            writer.writeBoolList(value);
        }
    
        @Override
        boolean[] getOrigionalValue(IonReader stream) {
            boolean[] result = new boolean[stream.getContainerSize()];
            stream.stepInto();
            for (int i = 0; i < result.length && stream.hasNext(); i++) {
                stream.next();
                result[i] = stream.booleanValue();
            }
            stream.stepOut();
            return result;
        }
        @Override
        boolean areEqual(boolean[] a, boolean[] b) {
            return a==null?b==null:Arrays.equals(a, b);
        }
        @Override
        boolean[] generateRandomValue(Random rand, int length, int depth) {
            boolean[] result = new boolean[length];
            for (int i=0;i<length;i++) {
                result[i] = rand.nextBoolean();
            }
            return result;
        }
    }
}