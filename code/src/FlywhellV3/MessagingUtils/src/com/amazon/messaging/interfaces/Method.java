package com.amazon.messaging.interfaces;


public interface Method <Result,Arg,Excep extends Throwable> {

    public Result call(Arg a) throws Excep;
    
}
