/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.dse.messaging.datagram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

abstract class AbstractSerializer<T> {
    double pnull = 0.1;

    int baseLength = 8;

    public AbstractSerializer() {
    }

    abstract void writeValue(T value, ByteArrayOutputStream writer, StatefullEncoder enc) throws IOException;

    abstract T getOrigionalValue(ByteBuffer stream, StatefullDecoder dec) throws IOException;

    T generateRandom(Random rand, int depth) {
        if (shouldBeNull(rand, pnull))
            return null;
        else {
            return generateRandomValue(rand, getLength(rand, baseLength), depth);
        }
    }

    private boolean shouldBeNull(Random rand, double pnull) {
        return rand.nextDouble() < pnull; // This cannot be <= because it will
                                            // cause a pnull value of 0 to
                                            // potentially return true.
    }

    private int getLength(Random rand, int baseLength) {
        return (int) Math.pow(baseLength, rand.nextFloat() * 2) - 1;
    }

    abstract T generateRandomValue(Random rand, int length, int depth);

    boolean areEqual(T a, T b) {
        if (a == null && b == null)
            return true;
        else if (a!=null && b!=null && a.equals(b))
            return true;
        else
            return false;
    }

    double getPnull() {
        return pnull;
    }

    void setPnull(double pnull) {
        this.pnull = pnull;
    }

    int getBaseLength() {
        return baseLength;
    }

    void setBaseLength(int baseLength) {
        this.baseLength = baseLength;
    }
}