package com.amazon.messaging.seqstore.v3;

import lombok.EqualsAndHashCode;

import com.google.common.base.Preconditions;

@EqualsAndHashCode
public class InflightUpdateRequest<T> {
    private T newInflightInfo;
    private boolean newInflightInfoSet = false;
    
    private long newTimeout;
    private boolean newTimeoutSet = false;
    
    private T expectedInflightInfo;
    private boolean expectedInflightInfoSet = false;
    
    public InflightUpdateRequest<T> withNewInflightInfo( T inflightInfo ) {
        this.newInflightInfo = inflightInfo;
        this.newInflightInfoSet = true;
        return this;
    }
    
    public InflightUpdateRequest<T> withNewTimeout( long timeout ) {
        Preconditions.checkArgument(timeout >= 0, "Timeout may not be negative");
        this.newTimeout = timeout;
        this.newTimeoutSet = true;
        return this;
    }
    
    public InflightUpdateRequest<T> withExpectedInflightInfo( T expectedInfo ) {
        this.expectedInflightInfo = expectedInfo;
        this.expectedInflightInfoSet = true;
        return this;
    }
    
    public T getNewInflightInfo() {
        return newInflightInfo;
    }

    public boolean isNewInflightInfoSet() {
        return newInflightInfoSet;
    }
    
    public long getNewTimeout() {
        return newTimeout;
    }
    
    public boolean isNewTimeoutSet() {
        return newTimeoutSet;
    }
    
    public T getExpectedInflightInfo() {
        return expectedInflightInfo;
    }

    public boolean isExpectedInflightInfoSet() {
        return expectedInflightInfoSet;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder( "InflightUpdateRequest [" );
        boolean first = true;
        
        if( newInflightInfoSet ) {
            builder.append( "newInflightInfo=" ).append ( newInflightInfo );
            first = false;
        }
        
        if( newTimeoutSet ) {
            if( first ) {
                builder.append( ", " );
            }
            builder.append( "newTimeout=" ).append( newTimeout );
        }
        
        if( expectedInflightInfoSet ) {
            if( first ) {
                builder.append( ", " );
            }
            builder.append( "expectedInflightInfo=" ).append( expectedInflightInfo );
        }
        
        builder.append ("]");
        return builder.toString();
    }
    
    
}
