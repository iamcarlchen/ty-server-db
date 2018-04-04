package com.greatbee.core.db.restapi.basecase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.db.restapi.RestAPIManagerTest;
import org.junit.Test;

/**
 * Created by usagizhang on 18/3/28.
 */
public abstract class BaseConnectTest extends RestAPIManagerTest implements ExceptionCode {

    public BaseConnectTest() throws DBException {
        this.setUp();
    }

    @Test
    public void startTest() throws DBException {
        this.runTest();
    }

    protected abstract void runTest() throws DBException;
}
