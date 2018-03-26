package com.greatbee.core.db.mysql.baseCase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.ConT;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.db.mysql.MysqlRelationalDataManagerTest;
import com.greatbee.core.util.ConnectorTreeUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 18/3/14.
 */
public abstract class BaseConnectorTreeTest extends MysqlRelationalDataManagerTest implements ExceptionCode {


    protected ConnectorTree queryTree;

    public BaseConnectorTreeTest() throws DBException {
        this.setUp();
        this.settingOIView();
        this.initConnectorTree();
    }

    @Test
    public void startTest() throws DBException {
        this.runTest(queryTree);
    }

    protected abstract void runTest(ConnectorTree connectorTree) throws DBException;

    public void initConnectorTree() {
        Connector connector = new Connector();
        connector.setAlias("test_connector");
        connector.setName("test_connector");
        connector.setDescription("test_connector");
        connector.setFromFieldName("categoryId");
        connector.setFromOIAlias(mainView.getOi().getAlias());
        connector.setToFieldName("categoryId");
        connector.setToOIAlias(subView.getOi().getAlias());
        List<ConnectorTree> connectorTreeList = new ArrayList<ConnectorTree>();
        connectorTreeList.add(ConnectorTreeUtil.buildConnectorTree(subView, connector));
        queryTree = ConnectorTreeUtil.buildConnectorTree(mainView, connector, connectorTreeList, ConT.Left);

    }


}
