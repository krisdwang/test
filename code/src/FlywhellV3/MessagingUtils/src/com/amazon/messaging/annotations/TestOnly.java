package com.amazon.messaging.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates this this method / class / field should only ever be referenced or
 * used by unit tests and should have no impact on the normal operation of the
 * system.
 * 
 * @author kaitchuc
 */
@Retention(RetentionPolicy.SOURCE)
@Target( { ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.CONSTRUCTOR })
public @interface TestOnly {

}
