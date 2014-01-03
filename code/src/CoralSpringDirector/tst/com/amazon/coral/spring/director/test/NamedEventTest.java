package com.amazon.coral.spring.director.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.amazon.coral.spring.director.event.NamedEvent;

public class NamedEventTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNonNullName() {
        new NamedEvent(new Object(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonNullSource() {
        new NamedEvent(null, "hi");
    }

    @Test
    public void testA() {
        Object source = new Object();

        NamedEvent e1 = new NamedEvent(source, "hi");
        NamedEvent e2 = new NamedEvent(source, "hi");

        assertEquals(e1, e2);

        assertFalse(e1.equals(new Object()));
        assertFalse(e2.equals(new NamedEvent(source, "foo")));
        assertFalse(e2.equals(new NamedEvent(new Object(), "hi")));
    }
}
