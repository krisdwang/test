package com.amazon.messaging.seqstore.v3.exceptions;

public class SeqStoreExceptionHelper {
    /**
     * Wraps the given SeqStoreException in a form suitable to be rethrown. This function
     * keeps {@link SeqStoreDatabaseException} and {@link SeqStoreUnrecoverableDatabaseException}
     * exceptions unchanged and wraps all others in {@link SeqStoreInternalException}
     * 
     * @param e
     * @return
     */
    public static SeqStoreException wrapException( String message, Exception e ) {
        if( e instanceof SeqStoreDatabaseException ) {
            return wrapException( message, ( SeqStoreDatabaseException ) e );
        } else {
            return new SeqStoreInternalException( message, e );
        }
    }
    
    public static SeqStoreDatabaseException wrapException( 
        String message, SeqStoreDatabaseException e ) 
    {
        if( e instanceof SeqStoreUnrecoverableDatabaseException ) {
            return new SeqStoreUnrecoverableDatabaseException(
                    ((SeqStoreUnrecoverableDatabaseException) e).getReason(), 
                    message, e );
        } else {
            return new SeqStoreDatabaseException( message, e );
        }
    }
}
