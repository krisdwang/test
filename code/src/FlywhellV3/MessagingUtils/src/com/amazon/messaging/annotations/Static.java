package com.amazon.messaging.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

@Retention(RetentionPolicy.SOURCE)
@Target( { ElementType.TYPE, ElementType.FIELD })

@ThreadSafe
@Immutable
public @interface Static {

}
