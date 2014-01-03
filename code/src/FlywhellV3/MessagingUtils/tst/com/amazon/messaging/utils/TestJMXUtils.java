package com.amazon.messaging.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import lombok.Data;

import org.junit.Test;

public class TestJMXUtils {
    @Test
    public void testNameMangling() throws MalformedObjectNameException, NullPointerException {
        final String badValue1 = "Bad\nValue\n1";
        final String badValue2 = "\"BadValue2=\n,:,\n:=*?\"?*\"\":";
        
        StringBuilder properties = new StringBuilder();
        
        properties.append( JMXUtils.urlEncodeKey( "Bad\nKey\n1" ) + "=" + JMXUtils.makeProperyValueSafe(badValue1) );
        properties.append( "," + JMXUtils.makeObjectNameProperty( "BadKey2_=,*?,*?::=\n", badValue2 ) );
        properties.append( "," + JMXUtils.makeObjectNameProperty( "NormalKey", "NormalValue") );
        
        ObjectName name = new ObjectName( "TestJMXUtils:" + properties.toString() );
        assertEquals( "NormalValue", name.getKeyProperty( "NormalKey") );
        assertEquals( badValue1, ObjectName.unquote( name.getKeyProperty( "Bad%0AKey%0A1" ) ) );
        assertEquals( badValue2, ObjectName.unquote( 
                name.getKeyProperty( "BadKey2_%3D%2C%2A%3F%2C%2A%3F%3A%3A%3D%0A" ) ) );
        
    }
    
    public static interface TestMBeanMBean {
        public int getValue();
    }
    
    @Data
    public static class TestMBean implements TestMBeanMBean {
        private int value;
    }
    
    @Test
    public void testRegister() throws AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException {
        TestMBean mbean = new TestMBean();
        mbean.setValue(5);
        
        ObjectName name = JMXUtils.registerMBean( "TestJMXUtils", "Type=TestMBean", mbean );
        assertNotNull( name );
        assertEquals( "TestMBean", name.getKeyProperty( "Type") );
        
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        
        Set<ObjectName> matchingNames = mBeanServer.queryNames(name, null);
        assertEquals( Collections.singleton(name), matchingNames );
        
        Object value = mBeanServer.getAttribute(name, "Value");
        assertNotNull( value );
        assertEquals( Integer.valueOf( 5 ), value );
        
        JMXUtils.unregisterMBean(name);
        matchingNames = mBeanServer.queryNames(name, null);
        assertEquals( Collections.emptySet(), matchingNames );
    }
}
