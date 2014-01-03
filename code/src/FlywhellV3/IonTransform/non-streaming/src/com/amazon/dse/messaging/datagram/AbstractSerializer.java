/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.dse.messaging.datagram;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;

abstract class AbstractSerializer<T>
{
    final IonSystem sys;
    public AbstractSerializer(IonSystem sys) {
        this.sys = sys;
    }
    IonValue getIonValue(T o, String Encoding) {
        return getIonValue(o);
    }
    abstract IonValue getIonValue(T o);
    abstract T getOrigionalValue(IonValue orig);
}