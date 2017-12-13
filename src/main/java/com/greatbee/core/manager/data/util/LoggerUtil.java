package com.greatbee.core.manager.data.util;

import com.greatbee.core.manager.data.ext.OracleDataManager;
import org.apache.log4j.Logger;

/**
 * Created by usagizhang on 17/12/13.
 */
public class LoggerUtil {
    private static Logger logger = null;

    private boolean isTest = true;

    public LoggerUtil(Class c) {
        logger = Logger.getLogger(c);

    }

    public void info(String _loging) {
        if (isTest) {
            System.out.println(_loging);
        } else {
            logger.info(_loging);
        }

    }

    public void error(String _loging) {
        if (isTest) {
            System.out.println(_loging);
        } else {
            logger.error(_loging);
        }
    }
}
