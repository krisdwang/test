package com.amazon.messaging.concurent;

import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.messaging.interfaces.Available;


public class NotifiedAvailable implements Available<Void, RuntimeException> {

    AtomicBoolean available = new AtomicBoolean();

    public void makeAvailable() {
        available.set(true);
    }
    
    @Override
    public boolean grabAvailable(Void v) {
        return available.compareAndSet(true, false);
    }

}
