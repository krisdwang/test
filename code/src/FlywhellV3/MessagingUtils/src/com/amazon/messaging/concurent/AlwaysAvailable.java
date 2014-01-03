package com.amazon.messaging.concurent;

import com.amazon.messaging.interfaces.Available;

/**
 * A dummy class that implements available for the case the the resource in
 * question is always available.
 * 
 * @author kaitchuc
 */
public class AlwaysAvailable implements Available<Void, RuntimeException> {

    @Override
    public boolean grabAvailable(Void v) {
        return true;
    }

}
