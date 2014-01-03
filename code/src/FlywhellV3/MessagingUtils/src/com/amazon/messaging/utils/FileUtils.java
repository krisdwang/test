package com.amazon.messaging.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import lombok.Cleanup;


public class FileUtils {
    public static void copyFile( File source, File dest ) throws IOException {
        @Cleanup InputStream inputStream = new FileInputStream( source );
        @Cleanup OutputStream outputStream = new FileOutputStream( dest );
        byte buffer[] = new byte[1024];
        for(;;) {
            int read = inputStream.read( buffer );
            if( read == -1 ) break;
            outputStream.write( buffer, 0, read );
        } 
        outputStream.flush();
    }
}
