package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.db.mysql.baseCase.BaseConnectorTreeTest;
import org.junit.Assert;

/**
 * Created by usagizhang on 18/3/14.
 */
public class TestCountByConnectorTree extends BaseConnectorTreeTest {

    public TestCountByConnectorTree() throws DBException {

    }

    @Override
    protected void runTest(ConnectorTree connectorTree) throws DBException {
        int count = this.mysqlDataManager.count(queryTree);
        Assert.assertTrue(count > 0);
        this.printJSONObject(count);
    }
}
