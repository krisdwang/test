package com.amazon.dse.messaging.datagram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import com.amazon.ion.stateless.ContainerReaders;
import com.amazon.ion.stateless.ContainerWriters;
import com.amazon.ion.stateless.ValueReaders;
import com.amazon.ion.stateless.ValueWriters;

/**
 * Serializing an object for known object types.
 */
class Serializers {

    private static byte[] getRandomBytes(Random rand, int length) {
        byte[] randBytes = new byte[length];
        rand.nextBytes(randBytes);
        return randBytes;
    }

    static final class ByteSerializer extends AbstractSerializer<Byte> {
        static final int[] CANONICAL_NAME = new int[]{14};

        static final ValueWriters.byteWriter writer = new ValueWriters.byteWriter();

        static final ValueReaders.byteReader reader = new ValueReaders.byteReader();

        @Override
        public final void writeValue(Byte value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(CANONICAL_NAME, value, out);
        }

        @Override
        Byte getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return (byte) reader.readValue(buff);
        }

        @Override
        Byte generateRandomValue(Random rand, int length, int depth) {
            return new Byte((byte) rand.nextInt());
        }
    }

    static final class ShortSerializer extends AbstractSerializer<Short> {
        static final int[] CANONICAL_NAME = new int[]{16};

        static final ValueWriters.shortWriter writer = new ValueWriters.shortWriter();

        static final ValueReaders.shortReader reader = new ValueReaders.shortReader();

        @Override
        public final void writeValue(Short value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(CANONICAL_NAME, value, out);
        }

        @Override
        Short getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return (short) reader.readValue(buff);
        }

        @Override
        Short generateRandomValue(Random rand, int length, int depth) {
            return new Short((short) rand.nextInt());
        }
    }

    static final class IntSerializer extends AbstractSerializer<Integer> {
        static final int[] CANONICAL_NAME = new int[]{20};

        static final ValueWriters.intWriter writer = new ValueWriters.intWriter();

        static final ValueReaders.intReader reader = new ValueReaders.intReader();

        @Override
        public final void writeValue(Integer value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(CANONICAL_NAME, value, out);
        }

        @Override
        Integer getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff);
        }

        @Override
        Integer generateRandomValue(Random rand, int length, int depth) {
            return new Integer(rand.nextInt());
        }
    }

    static final class LongSerializer extends AbstractSerializer<Long> {
        static final int[] CANONICAL_NAME = new int[]{24};

        static final ValueWriters.longWriter writer = new ValueWriters.longWriter();

        static final ValueReaders.longReader reader = new ValueReaders.longReader();

        @Override
        public final void writeValue(Long value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(CANONICAL_NAME, value, out);
        }

        @Override
        Long getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff);
        }

        @Override
        Long generateRandomValue(Random rand, int length, int depth) {
            return new Long(rand.nextLong());
        }
    }

//    static final class BigIntSerializer extends AbstractSerializer<BigInteger> {
//        static final int[] CANONICAL_NAME = new int[]{Math.abs(BigInteger.class.getCanonicalName().hashCode())};
//
//        @Override
//        public final void writeValue(BigInteger value, ByteArrayOutputStream out) throws IOException {
//            writer.writeValue(CANONICAL_NAME
//            writer.writeDecimal(new BigDecimal(value));
//        }
//
//        @Override
//        BigInteger getOrigionalValue(ByteBuffer buff) throws IOException {
//            throw new IllegalStateException("This should be handled by BigDecimalSerializer.");
//        }
//
//        @Override
//        BigInteger generateRandomValue(Random rand, int length, int depth) {
//            return new BigInteger(getRandomBytes(rand, length + 1));
//        }
//    }
//
//    static final class BigDecimalSerializer extends AbstractSerializer<Number> {
//
//        @Override
//        public final void writeValue(Number value, ByteArrayOutputStream out) throws IOException {
//            writer.writeDecimal((BigDecimal) value);
//        }
//
//        @Override
//        Number getOrigionalValue(ByteBuffer buff) throws IOException {
//            String[] annotations = reader.readValue(buff);
//            if (annotations == null || annotations.length <= 0
//                    || !BigInteger.class.getCanonicalName().hashCode().equals(annotations[0]))
//                return reader.readValue(buff);
//            else
//                return reader.readValue(buff);
//        }
//
//        @Override
//        BigDecimal generateRandomValue(Random rand, int length, int depth) {
//            BigInteger num = new BigInteger(getRandomBytes(rand, length + 1));
//            BigDecimal result = new BigDecimal(num);
//            return result;
//        }
//    }

    static final class DoubleSerializer extends AbstractSerializer<Double> {
        static final int[] CANONICAL_NAME = new int[]{32};

        static final ValueWriters.doubleWriter writer = new ValueWriters.doubleWriter();

        static final ValueReaders.doubleReader reader = new ValueReaders.doubleReader();

        @Override
        public final void writeValue(Double value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(CANONICAL_NAME
            ,value,out);
        }

        @Override
        Double getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff);
        }

        @Override
        Double generateRandomValue(Random rand, int length, int depth) {
            return Double.longBitsToDouble(rand.nextLong());
        }
    }

    static final class FloatSerializer extends AbstractSerializer<Float> {
        static final int[] CANONICAL_NAME = new int[]{28};

        static final ValueWriters.floatWriter writer = new ValueWriters.floatWriter();

        static final ValueReaders.floatReader reader = new ValueReaders.floatReader();

        @Override
        public final void writeValue(Float value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(CANONICAL_NAME
            ,value,out);
        }

        @Override
        Float getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return (float) reader.readValue(buff);
        }

        @Override
        Float generateRandomValue(Random rand, int length, int depth) {
            return Float.intBitsToFloat(rand.nextInt());
        }
    }

    static final class CharSerializer extends AbstractSerializer<Character> {
        static final int[] CANONICAL_NAME = new int[]{36};

        static final ValueWriters.stringWriter writer = new ValueWriters.stringWriter();

        static final ValueReaders.stringReader reader = new ValueReaders.stringReader();

        @Override
        public final void writeValue(Character value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(CANONICAL_NAME,value.toString(),out);
        }

        @Override
        public Character getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff).charAt(0);
        }

        @Override
        Character generateRandomValue(Random rand, int length, int depth) {
            char value;
            do {
                value = (char) (rand.nextInt(Character.MAX_VALUE));
            } while (!Character.isDefined(value) || Character.isHighSurrogate(value)
                    || Character.isLowSurrogate(value) || !Character.isValidCodePoint(value)
                    || Character.isSupplementaryCodePoint(value));
            return new Character((char) value);
        }
    }

    static final class DateSerializer extends AbstractSerializer<Date> {
        static final ValueWriters.dateWriter writer = new ValueWriters.dateWriter();

        static final ValueReaders.dateReader reader = new ValueReaders.dateReader();

        @Override
        public final void writeValue(Date value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(null,value,out);
        }

        @Override
        public Date getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff);
        }

        @Override
        Date generateRandomValue(Random rand, int length, int depth) {
            return new Date(rand.nextLong());
        }
    }

    static final class BooleanSerializer extends AbstractSerializer<Boolean> {
        static final ValueWriters.booleanWriter writer = new ValueWriters.booleanWriter();

        static final ValueReaders.booleanReader reader = new ValueReaders.booleanReader();

        @Override
        public final void writeValue(Boolean value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(null,value,out);
        }

        @Override
        Boolean getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff);
        }

        @Override
        Boolean generateRandomValue(Random rand, int length, int depth) {
            return rand.nextBoolean();
        }
    }

    static final class ByteArraySerializer extends AbstractSerializer<byte[]> {
        static final ValueWriters.blobWriter writer = new ValueWriters.blobWriter();

        static final ValueReaders.blobReader reader = new ValueReaders.blobReader();

        @Override
        public final void writeValue(byte[] value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(null,value,out);
        }

        @Override
        public byte[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff);
        }

        @Override
        boolean areEqual(byte[] a, byte[] b) {
            return a == null ? b == null : Arrays.equals(a, b);
        }

        @Override
        byte[] generateRandomValue(Random rand, int length, int depth) {
            return getRandomBytes(rand, length);
        }
    }

    static final class StringSerializer extends AbstractSerializer<String> {
        static final ValueWriters.stringWriter writer = new ValueWriters.stringWriter();

        static final ValueReaders.stringReader reader = new ValueReaders.stringReader();

        @Override
        public final void writeValue(String value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(null,value,out);
        }

        @Override
        public final String getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff);
        }

        @Override
        String generateRandomValue(Random rand, int length, int depth) {
            try {
                String result = new String(getRandomBytes(rand, length), "UTF-16");
                return result;
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static final class CharArraySerializer extends AbstractSerializer<char[]> {
        static final int[] CANONICAL_NAME = new int[]{37};

        static final ValueWriters.stringWriter writer = new ValueWriters.stringWriter();

        static final ValueReaders.stringReader reader = new ValueReaders.stringReader();

        @Override
        public final void writeValue(char[] value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            writer.writeValue(CANONICAL_NAME,String.valueOf(value),out);
        }

        @Override
        public char[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            return reader.readValue(buff).toCharArray();
        }

        @Override
        boolean areEqual(char[] a, char[] b) {
            return a == null ? b == null : Arrays.equals(a, b);
        }

        @Override
        char[] generateRandomValue(Random rand, int length, int depth) {
            try {
                return new String(getRandomBytes(rand, length), "UTF-16").toCharArray();
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static final class ShortArrayWriter extends AbstractSerializer<short[]> {
        static final int[] CANONICAL_NAME = new int[]{18};

        static final ValueWriters.shortWriter writer = new ValueWriters.shortWriter();
        static final ValueReaders.shortReader reader = new ValueReaders.shortReader();
        static final ContainerWriters.listWriter listWriter = new ContainerWriters.listWriter();
        static final ContainerReaders.listReader listReader = new ContainerReaders.listReader();

        @Override
        public final void writeValue(short[] value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            ByteArrayOutputStream array = new ByteArrayOutputStream();
            for (short v : value)
                writer.writeValue(null, v, array);
            listWriter.writeListStart(CANONICAL_NAME, array.size(), out);
            array.writeTo(out);
            listWriter.writeListEnd(out);
        }

        @Override
        short[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            int limit = buff.limit();
            short[] result = new short[listReader.readListStart(buff)];
            for (int i = 0; i < result.length; i++) {
                result[i] = (short) reader.readValue(buff);
            }
            listReader.readListEnd(limit, buff);
            return result;
        }

        @Override
        boolean areEqual(short[] a, short[] b) {
            return a == null ? b == null : Arrays.equals(a, b);
        }

        @Override
        short[] generateRandomValue(Random rand, int length, int depth) {
            short[] result = new short[length];
            for (int i = 0; i < length; i++) {
                result[i] = (short) rand.nextInt();
            }
            return result;
        }
    }

    static final class IntArrayWriter extends AbstractSerializer<int[]> {
        static final int[] CANONICAL_NAME = new int[]{22};

        static final ValueWriters.intWriter writer = new ValueWriters.intWriter();
        static final ValueReaders.intReader reader = new ValueReaders.intReader();
        static final ContainerWriters.listWriter listWriter = new ContainerWriters.listWriter();
        static final ContainerReaders.listReader listReader = new ContainerReaders.listReader();

        @Override
        public final void writeValue(int[] value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            ByteArrayOutputStream array = new ByteArrayOutputStream();
            for (int v : value)
                writer.writeValue(null, v, array);
            listWriter.writeListStart(CANONICAL_NAME, array.size(), out);
            array.writeTo(out);
            listWriter.writeListEnd(out);
        }

        @Override
        int[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            int limit = buff.limit();
            int[] result = new int[listReader.readListStart(buff)];

            for (int i = 0; i < result.length; i++) {
                result[i] = reader.readValue(buff);
            }
            listReader.readListEnd(limit, buff);
            return result;
        }

        @Override
        boolean areEqual(int[] a, int[] b) {
            return a == null ? b == null : Arrays.equals(a, b);
        }

        @Override
        int[] generateRandomValue(Random rand, int length, int depth) {
            int[] result = new int[length];
            for (int i = 0; i < length; i++) {
                result[i] = rand.nextInt();
            }
            return result;
        }
    }

    static final class LongArrayWriter extends AbstractSerializer<long[]> {
        static final int[] CANONICAL_NAME = new int[]{26};

        static final ValueWriters.longWriter writer = new ValueWriters.longWriter();

        static final ValueReaders.longReader reader = new ValueReaders.longReader();
        static final ContainerWriters.listWriter listWriter = new ContainerWriters.listWriter();
        static final ContainerReaders.listReader listReader = new ContainerReaders.listReader();
        @Override
        public final void writeValue(long[] value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            ByteArrayOutputStream array = new ByteArrayOutputStream();
            for (long v : value)
                writer.writeValue(null, v, array);
            listWriter.writeListStart(CANONICAL_NAME, array.size(), out);
            array.writeTo(out);
            listWriter.writeListEnd(out);
        }

        @Override
        long[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            int limit = buff.limit();
            long[] result = new long[listReader.readListStart(buff)];

            for (int i = 0; i < result.length; i++) {
                result[i] = reader.readValue(buff);
            }
            listReader.readListEnd(limit, buff);
            return result;
        }

        @Override
        boolean areEqual(long[] a, long[] b) {
            return a == null ? b == null : Arrays.equals(a, b);
        }

        @Override
        long[] generateRandomValue(Random rand, int length, int depth) {
            long[] result = new long[length];
            for (int i = 0; i < length; i++) {
                result[i] = rand.nextLong();
            }
            return result;
        }
    }

    static final class DoubleArrayWriter extends AbstractSerializer<double[]> {
        static final int[] CANONICAL_NAME = new int[]{34};

        static final ValueWriters.doubleWriter writer = new ValueWriters.doubleWriter();

        static final ValueReaders.doubleReader reader = new ValueReaders.doubleReader();
        static final ContainerWriters.listWriter listWriter = new ContainerWriters.listWriter();
        static final ContainerReaders.listReader listReader = new ContainerReaders.listReader();
        @Override
        public final void writeValue(double[] value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            ByteArrayOutputStream array = new ByteArrayOutputStream();
            for (double v : value)
                writer.writeValue(null, v, array);
            listWriter.writeListStart(CANONICAL_NAME, array.size(), out);
            array.writeTo(out);
            listWriter.writeListEnd(out);
        }

        @Override
        double[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            int limit = buff.limit();
            double[] result = new double[listReader.readListStart(buff)];

            for (int i = 0; i < result.length; i++) {
                result[i] = reader.readValue(buff);
            }
            listReader.readListEnd(limit, buff);
            return result;
        }

        @Override
        boolean areEqual(double[] a, double[] b) {
            return a == null ? b == null : Arrays.equals(a, b);
        }

        @Override
        double[] generateRandomValue(Random rand, int length, int depth) {
            double[] result = new double[length];
            for (int i = 0; i < length; i++) {
                result[i] = Double.longBitsToDouble(rand.nextLong());
            }
            return result;
        }
    }

    static final class FloatArrayWriter extends AbstractSerializer<float[]> {
        static final int[] CANONICAL_NAME = new int[]{30};

        static final ValueWriters.floatWriter writer = new ValueWriters.floatWriter();

        static final ValueReaders.floatReader reader = new ValueReaders.floatReader();
        static final ContainerWriters.listWriter listWriter = new ContainerWriters.listWriter();
        static final ContainerReaders.listReader listReader = new ContainerReaders.listReader();
        @Override
        public final void writeValue(float[] value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            ByteArrayOutputStream array = new ByteArrayOutputStream();
            for (float v : value)
                writer.writeValue(null, v, array);
            listWriter.writeListStart(CANONICAL_NAME, array.size(), out);
            array.writeTo(out);
            listWriter.writeListEnd(out);
        }

        @Override
        float[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            int limit = buff.limit();
            float[] result = new float[listReader.readListStart(buff)];

            for (int i = 0; i < result.length; i++) {
                result[i] = (float) reader.readValue(buff);
            }
            listReader.readListEnd(limit, buff);
            return result;
        }

        @Override
        boolean areEqual(float[] a, float[] b) {
            return a == null ? b == null : Arrays.equals(a, b);
        }

        @Override
        float[] generateRandomValue(Random rand, int length, int depth) {
            float[] result = new float[length];
            for (int i = 0; i < length; i++) {
                result[i] = Float.intBitsToFloat(rand.nextInt());
            }
            return result;
        }
    }

    static final class BooleanArrayWriter extends AbstractSerializer<boolean[]> {
        static final int[] CANONICAL_NAME = new int[]{13};

        static final ValueWriters.booleanWriter writer = new ValueWriters.booleanWriter();

        static final ValueReaders.booleanReader reader = new ValueReaders.booleanReader();
        static final ContainerWriters.listWriter listWriter = new ContainerWriters.listWriter();
        static final ContainerReaders.listReader listReader = new ContainerReaders.listReader();
        @Override
        public final void writeValue(boolean[] value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            ByteArrayOutputStream array = new ByteArrayOutputStream();
            for (boolean v : value)
                writer.writeValue(null, v, array);
            listWriter.writeListStart(CANONICAL_NAME, array.size(), out);
            array.writeTo(out);
            listWriter.writeListEnd(out);
        }

        @Override
        boolean[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            int limit = buff.limit();
            boolean[] result = new boolean[listReader.readListStart(buff)];

            for (int i = 0; i < result.length; i++) {
                result[i] = reader.readValue(buff);
            }
            listReader.readListEnd(limit, buff);
            return result;
        }

        @Override
        boolean areEqual(boolean[] a, boolean[] b) {
            return a == null ? b == null : Arrays.equals(a, b);
        }

        @Override
        boolean[] generateRandomValue(Random rand, int length, int depth) {
            boolean[] result = new boolean[length];
            for (int i = 0; i < length; i++) {
                result[i] = rand.nextBoolean();
            }
            return result;
        }
    }
}