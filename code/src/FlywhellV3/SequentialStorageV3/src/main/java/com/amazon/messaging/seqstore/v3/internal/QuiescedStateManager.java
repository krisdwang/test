package com.amazon.messaging.seqstore.v3.internal;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.exceptions.EnqueuesDisabledException;

import edu.umd.cs.findbugs.annotations.NonNull;

public class QuiescedStateManager {
    private static final Log log = LogFactory.getLog( QuiescedStateManager.class ); 
    
    private final String name;
    private final AtomicReference<String> reason = new AtomicReference<String>();
    
    /**
     * Create a new QuiescedStateManager with the given name. The name 
     * is used for debug output.
     * 
     * @param name the name of the manager.
     */
    public QuiescedStateManager(String name) {
        this.name = name;
    }

    /**
     * Change the state of this manager to not be quiesced. Does nothing if not currently 
     * quiesced.
     */
    public final void disableQuiescentMode() {
        String oldReason = reason.getAndSet( null );
        if( oldReason != null ) {
            log.info( "Enqueues Enabled for " + name + ". Were disabled for reason " + oldReason );
        }
    }

    /**
     * Change the state to quiesced with the given reason. The reason provided will be
     * thrown as the message of the {@link EnqueuesDisabledException} exception
     * if throwIfQuiesced is called.
     * 
     * @param newReason the reason for quiescing. Must be non-null. 
     */
    public final void enableQuiescentMode(@NonNull String newReason) {
        if( newReason == null ) {
            throw new IllegalArgumentException("enableQuiescentMode must be given a reason.");
        }
        
        String oldReason = reason.getAndSet( newReason );
        if( oldReason == null ) {
            log.info( "Enqueues Disabled for " + name + " with reason " + newReason );
        } else {
            log.info( "Enqueues Disabled reason for" + name + " changed from " + oldReason + " to " + newReason );
        }
    }
    
    /**
     * Return true if this manager is quiesced.
     */
    public boolean isQuiesced() {
        return reason.get() != null;
    }

    /**
     * Returns the reason this manager is quiesced, or null if it is not quiesced.
     */
    public String getQuiescedReason() {
        return reason.get();
    }
    
    /**
     * Throws EnqueuesDisabledException is this manager or its parent is quiesced. 
     */
    public void throwIfQuiesced() throws EnqueuesDisabledException {
        String currentReason = getQuiescedReason();
        if( currentReason != null ) {
            throw new EnqueuesDisabledException( currentReason );
        }
    }
}
