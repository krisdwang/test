package com.amazon.messaging.seqstore.v3.internal;

import java.util.regex.Pattern;

import lombok.EqualsAndHashCode;

import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

/**
 * A simple implementation of StoreId that uses ":" to separate the Group and
 * the Id to form the name.
 * 
 * @author kaitchuc, stevenso
 */
@EqualsAndHashCode(callSuper=false)
public class StoreIdImpl implements StoreId {
    private static final char SEPARATOR = ':';
    
    private static final String SEPERATOR_STRING = String.valueOf(SEPARATOR);
    
    private static final String ESCAPE_STRING = SEPERATOR_STRING + SEPERATOR_STRING;
    
    private static final Pattern SEPERATOR_PATTERN = 
        Pattern.compile( SEPERATOR_STRING, Pattern.LITERAL );
    
    private static final Pattern ESCAPE_PATTERN = 
        Pattern.compile( ESCAPE_STRING, Pattern.LITERAL );
    
    private final String group;

    private final String id;
    
    public static String getStoreName(String group, String id) {
        String escapedGroup = escapeGroup(group);
        
        if( id == null ) return escapedGroup;
        return escapedGroup + SEPARATOR + id;
    }

    public static String escapeGroup(String group) {
        String escapedGroup;

        if( group.indexOf( SEPARATOR ) != -1 ) {
            escapedGroup = SEPERATOR_PATTERN.matcher(group).replaceAll( ESCAPE_STRING );
        } else {
            escapedGroup = group;
        }
        
        return escapedGroup;
    }

    public static String getEscapedGroupFromStoreName(String storeName) {
        int index = storeName.indexOf(SEPARATOR);
        while( index != -1 && index < storeName.length() - 1 && storeName.charAt( index + 1 ) == SEPARATOR ) {
            index = storeName.indexOf(SEPARATOR, index + 2);
        }
        
        if (index == -1) {
            return storeName;
        } else {
            return storeName.substring(0, index);
        }
    }

    public static String unescapeGroup(String escapedGroup) {
        if( escapedGroup.indexOf( SEPERATOR_STRING ) != -1 ) {
            return  ESCAPE_PATTERN.matcher(escapedGroup).replaceAll(SEPERATOR_STRING);
        } else {
            return escapedGroup;
        }
    }

    public StoreIdImpl(String group, String id) {
        super();
        this.group = group;
        this.id = id;
    }

    public StoreIdImpl(String storeName) {
        String escapedGroup = getEscapedGroupFromStoreName(storeName);
        if( !escapedGroup.equals( storeName ) ) {
            id = storeName.substring( escapedGroup.length() + 1, storeName.length() );
        } else {
            id = null;
        }
        
        group = unescapeGroup(escapedGroup);
    }

    @Override
    public String getGroupName() {
        return group;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return getStoreName();
    }

    @Override
    public String getStoreName() {
        return getStoreName(group, id);
    }
}
