/*********************************************************************************
*
* <p>
* Perforce File Stats:
* <pre>
* $Id: //brazil/src/appgroup/appgroup/libraries/FBAMICDataAccessLayer/mainline/tst/com/amazon/fba/mic/dao/AbstractDaoTestCase.java#3 $
* $DateTime: 2012/12/19 04:33:58 $
* $Change: 6681544 $
* </pre>
* </p>
*
* @author $Author: wdong $
* @version $Revision: #3 $
*
* Copyright Notice
*
* This file contains proprietary information of Amazon.com.
* Copying or reproduction without prior written approval is prohibited.
*
* Copyright (c) 2012 Amazon.com.  All rights reserved.
*
*********************************************************************************/
package com.amazon.fba.mic.dao;

import java.io.File;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;

import amazon.platform.config.AppConfig;

/**
 * @author wdong
 */ 
public abstract class AbstractDaoTestCase{
    private static Logger LOGGER = null;

    @Before
    public void setUp() throws Exception {

        final String appConfigRootPath = new File(".").getAbsolutePath() + "/build/";

        System.setProperty("amazon.platform.config.validate", "false");
        final String[] args = new String[] { "--root=" + appConfigRootPath, "--domain=test", "--realm=NAFulfillment" };

        if (!AppConfig.isInitialized()) {
            AppConfig.initialize("FBADefectFeeTransactionCore", null, args);
        }

        // Setup the root if it isn't defined
        if (System.getProperty("root") == null) {
            System.setProperty("root", ".");
        }

        // Set up the logger, if it isn't defined
        if (LOGGER == null) {
            BasicConfigurator.configure();
            Logger.getRootLogger().setLevel(Level.WARN);
            LOGGER = Logger.getLogger(getClass());
        }

    }
}
