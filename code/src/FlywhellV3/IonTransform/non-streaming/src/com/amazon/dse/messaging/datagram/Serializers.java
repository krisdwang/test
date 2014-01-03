package com.amazon.dse.messaging.datagram;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonString;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;

/**
 * Serializing an object for known object types.
 */
class Serializers {

	static class ByteSerializer extends AbstractSerializer<Byte> {
        public ByteSerializer(IonSystem sys) {
            super(sys);
        }

        public IonValue getIonValue(Byte o) {
            IonValue result = sys.newInt(o);
            result.addTypeAnnotation(byte.class.getCanonicalName());
            return result;
        }

        @Override
        Byte getOrigionalValue(IonValue orig) {
            return (byte)((IonInt)orig).intValue();
        }
    }

    static class ShortSerializer extends AbstractSerializer<Short> {
        static class ShortArrayWriter extends AbstractSerializer<short[]> {
        
            private final AbstractSerializer<Short> serializer;

            public ShortArrayWriter(IonSystem sys, AbstractSerializer<Short> serializer) {
                super(sys);
                this.serializer = serializer;
            }
        
            @Override
            IonValue getIonValue(short[] o) {
                List<IonValue> list = new ArrayList<IonValue>(o.length);
                for (short s : o ) {
                    list.add(sys.newInt(s));
                }
                IonList result = sys.newList(list);
                //IonList result = sys.newList(o);
                result.addTypeAnnotation(short[].class.getCanonicalName());
                return result;
            }
        
            @Override
            short[] getOrigionalValue(IonValue orig) {
                IonList list = (IonList)orig;
                short[] result = new short[list.size()];
                for (int i = 0; i<result.length; i++) {
                    result[i] = serializer.getOrigionalValue(list.get(i));
                }              
                return result;
            }
        
        }

        public ShortSerializer(IonSystem sys) {
            super(sys);
        }

        public IonValue getIonValue(Short o) {
            IonValue result = sys.newInt(o);
            result.addTypeAnnotation(short.class.getCanonicalName());
            return result;
        }

        @Override
        Short getOrigionalValue(IonValue orig) {
            return (short)((IonInt)orig).intValue();
        }
    }

    static class IntSerializer extends AbstractSerializer<Integer> {
        static class IntArrayWriter extends AbstractSerializer<int[]> {
        
            private final AbstractSerializer<Integer> serializer;

            public IntArrayWriter(IonSystem sys, AbstractSerializer<Integer> serializer) {
                super(sys);
                this.serializer = serializer;
            }
        
            @Override
            IonValue getIonValue(int[] o) {
                IonList result = sys.newList(o);
                result.addTypeAnnotation(int[].class.getCanonicalName());
                return result;
            }
        
            @Override
            int[] getOrigionalValue(IonValue orig) {
                IonList list = (IonList)orig;
                int[] result = new int[list.size()];
                for (int i = 0; i<result.length; i++) {
                    result[i] = serializer.getOrigionalValue(list.get(i));
                }              
                return result;
            }
        
        }

        public IntSerializer(IonSystem sys) {
            super(sys);
        }

        public IonValue getIonValue(Integer o) {
            IonValue result = sys.newInt(o);
            result.addTypeAnnotation(int.class.getCanonicalName());
            return result;
        }

        @Override
        Integer getOrigionalValue(IonValue orig) {
            return ((IonInt)orig).intValue();
        }
    }

    static class LongSerializer extends AbstractSerializer<Long> {
        static class LongArrayWriter extends AbstractSerializer<long[]> {
        
            private final AbstractSerializer<Long> serializer;

            public LongArrayWriter(IonSystem sys, AbstractSerializer<Long> serializer) {
                super(sys);
                this.serializer = serializer;
            }
        
            @Override
            IonValue getIonValue(long[] o) {
                IonList result = sys.newList(o);
                result.addTypeAnnotation(long[].class.getCanonicalName());
                return result;
            }
        
            @Override
            long[] getOrigionalValue(IonValue orig) {
                IonList list = (IonList)orig;
                long[] result = new long[list.size()];
                for (int i = 0; i<result.length; i++) {
                    result[i] = serializer.getOrigionalValue(list.get(i));
                }              
                return result;
            }
        
        }

        public LongSerializer(IonSystem sys) {
            super(sys);
        }

        public IonValue getIonValue(Long o) {
            IonValue result = sys.newInt(o);
            result.addTypeAnnotation(long.class.getCanonicalName());
            return result;
        }

        @Override
        Long getOrigionalValue(IonValue orig) {
            return ((IonInt)orig).longValue();
        }
    }
    static class DoubleSerializer extends AbstractSerializer<Double> {
        static class DoubleArrayWriter extends AbstractSerializer<double[]> {
        
            private final AbstractSerializer<Double> serializer;

            public DoubleArrayWriter(IonSystem sys, AbstractSerializer<Double> serializer) {
                super(sys);
                this.serializer = serializer;
            }
        
            @Override
            IonValue getIonValue(double[] o) {
                List<IonValue> list = new ArrayList<IonValue>(o.length);
                for (double f : o ) {
                    list.add(sys.newFloat(f));
                }
                IonList result = sys.newList(list);
//                IonList result = sys.newList(o);
                result.addTypeAnnotation(double[].class.getCanonicalName());
                return result;
            }
        
            @Override
            double[] getOrigionalValue(IonValue orig) {
                IonList list = (IonList)orig;
                double[] result = new double[list.size()];
                for (int i = 0; i<result.length; i++) {
                    result[i] = serializer.getOrigionalValue(list.get(i));
                }              
                return result;
            }
        
        }

        public DoubleSerializer(IonSystem sys) {
            super(sys);
        }

        public IonValue getIonValue(Double o) {
            IonFloat result = sys.newFloat(o);
            result.addTypeAnnotation(double.class.getCanonicalName());
            return result;
        }

        @Override
        Double getOrigionalValue(IonValue orig) {
            return ((IonFloat)orig).doubleValue();
        }
    }
    static class FloatSerializer extends AbstractSerializer<Float> {
        static class FloatArrayWriter extends AbstractSerializer<float[]> {
        
            private final AbstractSerializer<Float> serializer;

            public FloatArrayWriter(IonSystem sys, AbstractSerializer<Float> serializer) {
                super(sys);
                this.serializer = serializer;
            }
        
            @Override
            IonValue getIonValue(float[] o) {
                List<IonValue> list = new ArrayList<IonValue>(o.length);
                for (float f : o ) {
                    list.add(sys.newFloat(f));
                }
                IonList result = sys.newList(list);
              //  IonList result = sys.newList(o);
                result.addTypeAnnotation(float[].class.getCanonicalName());
                return result;
            }
        
            @Override
            float[] getOrigionalValue(IonValue orig) {
                IonList list = (IonList)orig;
                float[] result = new float[list.size()];
                for (int i = 0; i<result.length; i++) {
                    result[i] = serializer.getOrigionalValue(list.get(i));
                }              
                return result;
            }
        
        }

        public FloatSerializer(IonSystem sys) {
            super(sys);
        }

        public IonValue getIonValue(Float o) {
            IonFloat result = sys.newFloat(o);
            result.addTypeAnnotation(float.class.getCanonicalName());
            return result;
        }

        @Override
        Float getOrigionalValue(IonValue orig) {
            return ((IonFloat)orig).floatValue();
        }
    }
    
    static class CharSerializer extends AbstractSerializer<Character> {
        public CharSerializer(IonSystem sys) {
            super(sys);
        }
        public IonValue getIonValue(Character o, String encoding) {
            try {
                return sys.newClob(o.toString().getBytes(encoding));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        public Character getOrigionalValue(IonValue orig) {
            String result = ((IonClob) orig).stringValue(Charset.forName(orig.getTypeAnnotations()[0]));
            return result.charAt(0);
        }
        @Override
        IonValue getIonValue(Character o) {
            throw new IllegalArgumentException("Method not supported.");
        }
    }
    
    static class DateSerializer extends AbstractSerializer<Date> {
        public DateSerializer(IonSystem sys) {
            super(sys);
        }
        public IonValue getIonValue(Date o) {
            return sys.newUtcTimestamp(o);
        }
        public Date getOrigionalValue(IonValue orig) {
            return ((IonTimestamp)orig).dateValue();
        }
    }

    static class BooleanSerializer extends AbstractSerializer<Boolean> {
        static class BooleanArrayWriter extends AbstractSerializer<boolean[]> {
        
            private final AbstractSerializer<Boolean> serializer;

            public BooleanArrayWriter(IonSystem sys, AbstractSerializer<Boolean> serializer) {
                super(sys);
                this.serializer = serializer;
            }
        
            @Override
            IonValue getIonValue(boolean[] o) {
                List<IonValue> list = new ArrayList<IonValue>(o.length);
                for (boolean b : o ) {
                    list.add(sys.newBool(b));
                }
                IonList result = sys.newList(list);
                //IonList result = sys.newList(o);
                result.addTypeAnnotation(boolean.class.getCanonicalName());
                return result;
            }
        
            @Override
            boolean[] getOrigionalValue(IonValue orig) {
                IonList list = (IonList)orig;
                boolean[] result = new boolean[list.size()];
                for (int i = 0; i<result.length; i++) {
                    result[i] = serializer.getOrigionalValue(list.get(i));
                }              
                return result;
            }
        
        }
        public BooleanSerializer(IonSystem sys) {
            super(sys);
        }
        public IonValue getIonValue(Boolean o) {
            return sys.newBool(o);
        }
        public Boolean getOrigionalValue(IonValue orig) {
            return ((IonBool)orig).booleanValue();
        }
    }

    static class ByteArraySerializer extends AbstractSerializer<byte[]> {

        public ByteArraySerializer(IonSystem sys) {
            super(sys);
        }

        @Override
        IonValue getIonValue(byte[] o) {
            IonBlob result = sys.newNullBlob();
            result.setBytes(o);
            return result;
        }

        @Override
        byte[] getOrigionalValue(IonValue orig) {
            return ((IonBlob)orig).newBytes();
        }
    }
    
    static class StringSerializer extends AbstractSerializer<String> {
        public StringSerializer(IonSystem sys) {
            super(sys);
        }
        public IonValue getIonValue(String o, String Encoding) {
            return sys.newString(o);
        }
        public String getOrigionalValue(IonValue orig) {
            return ((IonString)orig).stringValue();
        }
        @Override
        IonValue getIonValue(String o) {
            return sys.newString(o);
          //  throw new IllegalArgumentException("Method not supported.");
        }
    }
    
    static class CharArraySerializer extends AbstractSerializer<char[]> {

        public CharArraySerializer(IonSystem sys) {
            super(sys);
        }

        @Override
        IonValue getIonValue(char[] o) {
            return sys.newString(String.copyValueOf(o));
        }

        @Override
        char[] getOrigionalValue(IonValue orig) {
            throw new IllegalArgumentException("Method not supported.");
        }
    }
}