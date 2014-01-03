package com.amazon.messaging.seqstore;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

public class SeqStoreTestUtils {

    private static final String DATABASE_DIR_PROPERTY = "bdbdir";

    private static final Logger log = Logger.getLogger(SeqStoreTestUtils.class);

    public static void clearDirectory(File directory) throws IOException {
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                clearDirectory(file);
            }
            if (!file.delete()) {
                throw new IOException("Error cleaning up directory " + directory.getAbsolutePath()
                        + " could not delete " + file.getName());
            }
        }
    }

    public static File getBDBDir(String subdir) {
        File bdbdir;
        if (System.getProperty(DATABASE_DIR_PROPERTY) != null) {
            bdbdir = new File(System.getProperty(DATABASE_DIR_PROPERTY) + "/" + subdir);
        } else {
            bdbdir = new File("testdata/" + subdir); // default under current
                                                     // directory
        }

        if (bdbdir.exists() && !bdbdir.isDirectory()) {
            throw new RuntimeException("Test output directory " + bdbdir.getAbsolutePath()
                    + " is not a directory.");
        }

        return bdbdir;
    }

    public static File setupBDBDir(String subdir) throws IOException {
        File bdbdir = getBDBDir(subdir);

        if (bdbdir.exists()) {
            log.warn("Clearing directory " + bdbdir);
            clearDirectory(bdbdir);
        } else if (!bdbdir.mkdirs()) {
            throw new IOException("Could not create test directory " + bdbdir.getAbsolutePath());
        }

        return bdbdir;
    }

}
