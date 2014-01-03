package com.amazon.messaging.interfaces;


public interface Available<Info,Except extends Throwable> {

    public boolean grabAvailable(Info additionalInformation) throws Except;
   
    
}
