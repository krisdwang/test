/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.dse.messaging.datagram;

import java.io.IOException;
import java.util.Random;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;

abstract class AbstractSerializer<T> {
    double pnull = 0.1;

    int baseLength = 10;

    public AbstractSerializer() {
    }

    abstract void writeValue(T value, IonWriter writer) throws IOException;

    abstract T getOrigionalValue(IonReader stream) throws IOException;

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
        return a == null ? b == null : a.equals(b);
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