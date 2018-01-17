package com.greatbee.core.db.sqlserver.testcase;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.DataList;
import com.greatbee.core.bean.constant.ConT;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.db.RelationalDataManager;
import com.greatbee.core.util.ConnectorTreeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 17/12/21.
 */
public class ListByConnectorTree {
    public ListByConnectorTree(OIView oiView, RelationalDataManager dataManager) throws DBException {
        DataList dataList = dataManager.list(ConnectorTreeUtil.buildConnectorTree(oiView));
        System.out.println("Data -> " + JSONObject.toJSONString(dataList));
    }


    public ListByConnectorTree(OIView userOIView, OIView accountOIView, RelationalDataManager dataManager) throws DBException {

        Connector connector = new Connector();
        connector.setAlias(userOIView.getOi().getAlias() + "_" + accountOIView.getOi().getAlias());
        connector.setFromFieldName("alias");
        connector.setFromOIAlias(userOIView.getOi().getAlias());
        connector.setToFieldName("userAlias");
        connector.setToOIAlias(accountOIView.getOi().getAlias());

        List<ConnectorTree> connectorTreeList = new ArrayList<ConnectorTree>();
        connectorTreeList.add(ConnectorTreeUtil.buildConnectorTree(accountOIView, connector));
        ConnectorTree queryTree = ConnectorTreeUtil.buildConnectorTree(userOIView, connector, connectorTreeList, ConT.Left);

        DataList dataList = dataManager.list(queryTree);
        System.out.println("Data -> " + JSONObject.toJSONString(dataList));
    }
}
