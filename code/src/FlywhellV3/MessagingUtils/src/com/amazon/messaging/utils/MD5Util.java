package com.amazon.messaging.utils;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;


public class MD5Util {
    /**
     * Size in bytes of an MD5 sum
     */
    public static final int MD5_LENGTH = 16;
    
    private static final MessageDigest MD5_DIGEST_BASE;

    static {
        try {
            MD5_DIGEST_BASE = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            throw new RuntimeException("Could not find MD5 digest");
        } 
        
        try {
            MD5_DIGEST_BASE.clone();
        } catch( CloneNotSupportedException e ) {
            throw new RuntimeException("MD5 digest is not cloneable");
        }
    }
    
    public static byte[] getMD5(byte[] input) {
        return getMD5Digest().digest(input);
    }
    
    public static byte[] getMD5(byte[] input, int offest, int length) {
        MessageDigest digest = getMD5Digest();
        digest.update(input, offest, length);
        return digest.digest();
    }
    
    /**
     * Get the MD5 directly into out.
     * 
     * @throws IllegalArgumentException if out does not have space to store the MD5 
     */
    public static void getMD5(byte[] input, int offest, int length, byte[] out, int outOffset) {
        if( out.length - outOffset < MD5_LENGTH ) {
            throw new IllegalArgumentException("Not enough room in out to store the MD5." );
        }
        
        MessageDigest digest = getMD5Digest();
        digest.update(input, offest, length);
        try {
            digest.digest(out, outOffset, MD5_LENGTH);
        } catch (DigestException e) {
            // This should never happen as we've checked the arguments before calling digest
            throw new RuntimeException( "Failed getting MD5", e);
        }
    }
    
    public static byte[] getMD5(ByteBuffer input) {
        MessageDigest digest = getMD5Digest();
        
        digest.update(input);
        return digest.digest();
    }

    private static MessageDigest getMD5Digest() {
        MessageDigest digest;
        try {
             digest = (MessageDigest) MD5_DIGEST_BASE.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalArgumentException("Failing cloning MD5 digest");
        }
        return digest;
    }
    
    private static final char[] HEX_CHARS = {'0', '1', '2', '3',
                                             '4', '5', '6', '7',
                                             '8', '9', 'a', 'b',
                                             'c', 'd', 'e', 'f',};

    /**
     * Get string version of an MD5 from the provided buffer.
     * @param buffer the buffer containing the md5
     * @return The string version of the MD5, e.g. 932c5c46732155b92c80479da2f69280
     */
    public static String formatMD5(byte[]buffer) {
        StringBuilder result = new StringBuilder(MD5_LENGTH * 2);
        for( byte bt : buffer ) {
            result.append( HEX_CHARS[(bt >>> 4) & 0xf] );
            result.append( HEX_CHARS[bt & 0xf] );
        }
        return result.toString();
    }
    
    /**
     * Get string version of an MD5 from the provided buffer. This function consumes the
     * contents of the buffer.
     * @param buffer the buffer containing the md5
     * @return The string version of the MD5, e.g. 932c5c46732155b92c80479da2f69280
     */
    public static String formatMD5(ByteBuffer buffer) {
        StringBuilder result = new StringBuilder(MD5_LENGTH * 2);
        while( buffer.hasRemaining() ) {
            byte bt = buffer.get();
            result.append( HEX_CHARS[(bt >>> 4) & 0xf] );
            result.append( HEX_CHARS[bt & 0xf] );
        }
        return result.toString();
    }
}
