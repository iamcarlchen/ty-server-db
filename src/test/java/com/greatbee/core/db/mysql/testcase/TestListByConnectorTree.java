package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.DataList;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.ConT;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.db.mysql.baseCase.BaseConnectorTreeTest;
import com.greatbee.core.util.ConnectorTreeUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 18/3/14.
 */
public class TestListByConnectorTree extends BaseConnectorTreeTest implements ExceptionCode {

    public TestListByConnectorTree() throws DBException {

    }

    @Override
    protected void runTest(ConnectorTree connectorTree) throws DBException {
        DataList list = this.mysqlDataManager.list(connectorTree);
        printJSONObject(list);
    }
}
