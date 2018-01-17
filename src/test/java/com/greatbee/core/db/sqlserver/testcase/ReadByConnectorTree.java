package com.greatbee.core.db.sqlserver.testcase;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
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
public class ReadByConnectorTree {
    public ReadByConnectorTree(OIView oiView, RelationalDataManager dataManager) throws DBException {

        Data data = dataManager.read(ConnectorTreeUtil.buildConnectorTree(oiView));
        System.out.println("Data -> " + JSONObject.toJSONString(data));
    }

    public ReadByConnectorTree(OIView mainView,OIView subView,RelationalDataManager dataManager) throws DBException{
        Connector connector = new Connector();
        connector.setAlias(mainView.getOi().getAlias() + "_" + subView.getOi().getAlias());
        connector.setFromFieldName("alias");
        connector.setFromOIAlias(mainView.getOi().getAlias());
        connector.setToFieldName("userAlias");
        connector.setToOIAlias(subView.getOi().getAlias());

        List<ConnectorTree> connectorTreeList = new ArrayList<ConnectorTree>();
        connectorTreeList.add(ConnectorTreeUtil.buildConnectorTree(subView, connector));
        ConnectorTree queryTree = ConnectorTreeUtil.buildConnectorTree(mainView, connector, connectorTreeList, ConT.Left);

        Data data = dataManager.read(queryTree);
        System.out.println("Data -> " + JSONObject.toJSONString(data));
    }
}
