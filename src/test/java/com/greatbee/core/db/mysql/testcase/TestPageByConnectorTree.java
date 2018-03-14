package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.DataPage;
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
public class TestPageByConnectorTree extends BaseConnectorTreeTest implements ExceptionCode {


    public TestPageByConnectorTree() throws DBException {
    }

    @Override
    protected void runTest(ConnectorTree connectorTree) throws DBException {
        DataPage page = this.mysqlDataManager.page(1, 5, queryTree);
        this.printJSONObject(page);
    }
}
