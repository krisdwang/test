package com.amazon.coral.spring.director.test;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.context.ApplicationEvent;

import com.amazon.coral.spring.director.event.EventStrategy;
import com.amazon.coral.spring.director.event.NamedEvent;
import com.amazon.coral.spring.director.event.NamedEventDispatcher;

public class NamedEventDispatcherTest {
    @Test
    public void testA() {
        final byte[] callMask = new byte[1];
        callMask[0] = 0x00;

        @SuppressWarnings("unused")
        Object target = new Object() {
            public void acquire() {
                callMask[0] |= 0x01;
            }

            public void activate() {
                callMask[0] |= 0x02;
            }

            public void deactivate() {
                callMask[0] |= 0x04;
            }

            public void release() {
                callMask[0] |= 0x08;
            }
        };

        NamedEventDispatcher ned = new NamedEventDispatcher();
        ned.setTarget(target);
        assertTrue(ned.getTarget() == target);

        ned.onApplicationEvent(new NamedEvent(this, "acquire"));
        assertTrue(callMask[0] == 0x01);

        ned.onApplicationEvent(new NamedEvent(this, "activate"));
        assertTrue(callMask[0] == 0x03);

        ned.onApplicationEvent(new NamedEvent(this, "deactivate"));
        assertTrue(callMask[0] == 0x07);

        ned.onApplicationEvent(new NamedEvent(this, "release"));
        assertTrue(callMask[0] == 0x0F);
    }

    @Test
    public void testB() {
        final Exception ex = new Exception("I refuse to be here");

        final byte[] callMask = new byte[1];
        callMask[0] = 0x00;

        @SuppressWarnings("unused")
        Object target = new Object() {
            public void foo() throws Exception {
                callMask[0] |= 0x01;
            }

            public void gar() throws Exception {
                throw ex;
            }
        };

        EventStrategy eventStrategy = new EventStrategy() {
            @Override
            public void onEvent(Object arg0, ApplicationEvent arg1) {
                callMask[0] |= 0x02;
            }
        };

        NamedEventDispatcher ned = new NamedEventDispatcher();
        ned.setTarget(target);
        ned.setUnknownEventStrategy(eventStrategy);
        assertTrue(ned.getTarget() == target);

        ned.onApplicationEvent(new NamedEvent(this, "foo"));
        assertTrue(callMask[0] == 0x01);

        ned.onApplicationEvent(new NamedEvent(this, "bar"));
        assertTrue(callMask[0] == 0x03);

        try {
            ned.onApplicationEvent(new NamedEvent(this, "gar"));
        } catch (RuntimeException e) {
            callMask[0] |= 0x04;
        }

        assertTrue(callMask[0] == 0x07);
    }
}
