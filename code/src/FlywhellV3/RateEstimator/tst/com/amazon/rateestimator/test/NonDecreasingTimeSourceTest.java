package com.amazon.rateestimator.test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.amazon.rateestimator.NonDecreasingTimeHelper;

public class NonDecreasingTimeSourceTest {

    @Test
    public void test() {
        NonDecreasingTimeHelper ndts = new NonDecreasingTimeHelper();
        assertEquals(1000.0, ndts.adjustTime(1000));
        assertEquals(1001.0, ndts.adjustTime(1001));
        assertEquals(2000.0, ndts.adjustTime(2000));
        assertEquals(2000.0, ndts.adjustTime(1000));
        assertEquals(2001.0, ndts.adjustTime(1001));
        assertEquals(3001.0, ndts.adjustTime(2001));
    }
}
