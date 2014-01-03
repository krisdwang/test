package com.amazon.coral.spring.director.test.util;

import java.io.File;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class SpringUtil {
    private static String fullFilename(String file) {
        return new File(new File(System.getProperty("basedir"), "tst"), file).toURI().toString();
    }

    public static AbstractApplicationContext getContext(String name) {
        return new FileSystemXmlApplicationContext(fullFilename(name));
    }
}
