package com.greatbee.core.manager;

import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.manager.data.ext.OracleDataManager;

/**
 * Oracle Data Manager
 * <p/>
 * Author: CarlChen
 * Date: 2017/11/21
 */
public class OracleDataManagerTest extends DBBaseTest {
    private OracleDataManager oracleDataManager;

    public void setUp() {
        super.setUp();

        oracleDataManager = (OracleDataManager) context.getBean("oracleDataManager");
    }

    /**
     * Test Read
     *
     * @throws DBException
     */
    public void testRead() throws DBException {
        ConnectorTree connectorTree = new ConnectorTree();
        oracleDataManager.read(connectorTree);
    }
}
