package com.amazon.messaging.utils;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.URLEncoder;
import java.util.regex.Pattern;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.umd.cs.findbugs.annotations.CheckForNull;


public class JMXUtils {
    private static final Log log = LogFactory.getLog(JMXUtils.class);
    
    private static final Pattern invalidValuePattern = Pattern.compile("[\n,=:*?\"]");
    
    public static String urlEncodeKey( String key ) {
        String result;
        try {
            result = URLEncoder.encode( key, "UTF-8" );
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError( "UTF-8 encoding not supported?" );
        }
        result = result.replace("*", "%2A"); // replace * which default url encoding ignores.
        
        return result;
    }
    
    public static String makeProperyValueSafe( String str ) {
        if( invalidValuePattern.matcher( str ).find() ) {
            str = ObjectName.quote(str);
        }
        
        return str;
    }
    
    /**
     * Make a property value string for an object name replacing bad characters 
     * in the key and value as necessary.
     * <p>
     * The key is made safe by URL encoding it with the '*' character also being
     * URL encoded which the default URL encoder leaves unchanged.
     * <p>
     * The value is made safe by quoting it using ObjectName.quote if it contains
     * any of the characters not legally allowed in an ObjectName value.
     *  
     * @param key
     * @param value
     * @return
     */
    public static String makeObjectNameProperty(String key, String value) {
        return urlEncodeKey( key ) + "=" + makeProperyValueSafe( value );
    }
    
    /**
     * Register an mbean. Returns the name of the mbean or null if the registration failed for any reason.
     * 
     * @param domain the domain for the mbean
     * @param properties the properties for the mbean. Should be a valid properties string for an ObjectName
     * @param mbean the bean to register
     * @return the name for the bean, or null if the bean was not registered for any reason.
     */
    @CheckForNull
    public static ObjectName registerMBean( String domain, String properties, Object mbean ) {
        ObjectName retval = null;
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        if( mBeanServer != null ) {
            try {
                retval = new ObjectName( domain + ":" + properties );
            } catch (MalformedObjectNameException e) {
                log.warn( "Invalid domain or properties when trying to register mbean. Domain = " + domain + " properties = " + properties, e );
                return null;
            }
            
            try {
                mBeanServer.registerMBean( mbean, retval );
            } catch (JMException e) {
                log.warn( "Failed registering mbean " + retval, e );
                retval = null;
            }
        } else {
            log.info( "Not registering " + domain + ":" + properties + " as there is no mbean server.");
        }
        return retval;
    }
    
    /**
     * Unregister the specified MBean. Does nothing if name is null. If the unregistration fails
     * the exception is logged and ignored.
     * 
     * @param name the name of the bean to unregister.
     */
    public static void unregisterMBean( ObjectName name ) {
        if( name == null ) return;
        
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.unregisterMBean(name);
        } catch (InstanceNotFoundException e) {
            log.warn( "Tried to unregistering non-existent bean" + name);
        } catch (MBeanRegistrationException e) {
            log.warn( "MBean deregistration failed for bean " + name);
        }
    }
}
