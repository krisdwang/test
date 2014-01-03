package com.amazon.messaging.annotations;

import java.lang.annotation.Annotation;

import net.jcip.annotations.Immutable;

public class Validator {

    public static <X> void validateAnnotations(Class<X> testClass) {
        Annotation[] annotations = testClass.getAnnotations();
        if (isX(annotations, Stateful.class)) {
            if (isX(annotations, Immutable.class))
                throw new IllegalStateException("Class " + testClass
                        + " claims to be both statefull and immutable.");
            if (isX(annotations, ErrorHandler.class))
                throw new IllegalStateException("Class " + testClass
                        + " should not be a statefull error handler.");
        }

    }

    private static boolean isX(Annotation[] annotations, Class<? extends Annotation> X) {
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(X)) {
                return true;
            }
        }
        return false;
    }

    // private static boolean isRelevent(Annotation annotation) {
    // return annotation.annotationType().equals(Blocking.class) ||
    // annotation.annotationType().equals(ErrorHandler.class)
    // || annotation.annotationType().equals(Immutable.class) ||
    // annotation.annotationType().equals(Stateful.class);
    // }

}
