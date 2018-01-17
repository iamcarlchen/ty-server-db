package com.greatbee.core.db.sqlserver.testcase;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.DataPage;
import com.greatbee.core.bean.constant.ConT;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.data.RelationalDataManager;
import com.greatbee.core.manager.utils.ConnectorTreeUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 17/12/21.
 */
public class PageByConnectorTree {
    public PageByConnectorTree(OIView oiView, RelationalDataManager dataManager) throws DBException {
        DataPage dataPage = dataManager.page(1, 10, ConnectorTreeUtil.buildConnectorTree(oiView));
        System.out.println("Data -> " + JSONObject.toJSONString(dataPage));
    }

    public PageByConnectorTree(OIView mainView,OIView subView,RelationalDataManager dataManager) throws DBException {

        Connector connector = new Connector();
        connector.setAlias(mainView.getOi().getAlias() + "_" + subView.getOi().getAlias());
        connector.setFromFieldName("alias");
        connector.setFromOIAlias(mainView.getOi().getAlias());
        connector.setToFieldName("userAlias");
        connector.setToOIAlias(subView.getOi().getAlias());

        List<ConnectorTree> connectorTreeList = new ArrayList<ConnectorTree>();
        connectorTreeList.add(ConnectorTreeUtil.buildConnectorTree(subView, connector));
        ConnectorTree queryTree = ConnectorTreeUtil.buildConnectorTree(mainView, connector, connectorTreeList, ConT.Left);


        DataPage dataPage = dataManager.page(1, 10, queryTree);
        System.out.println("Data -> " + JSONObject.toJSONString(dataPage));
    }


}
