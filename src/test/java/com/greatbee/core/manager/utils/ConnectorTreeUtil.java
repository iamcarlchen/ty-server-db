package com.greatbee.core.manager.utils;

import com.greatbee.core.bean.constant.ConT;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.OIView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by usagizhang on 17/12/28.
 */
public class ConnectorTreeUtil {

    public static ConnectorTree buildConnectorTree(OIView mainView, Connector connector, List<ConnectorTree> subTreeView, ConT conT) {
        ConnectorTree queryTree = buildConnectorTree(mainView);
        queryTree.setConnector(connector);
        queryTree.setConT(conT);
        queryTree.setChildren(subTreeView);
        return queryTree;
    }

    public static ConnectorTree buildConnectorTree(OIView mainView) {
        Map<String, Field> queryField = OIViewUtil.getOIField(mainView);
        ConnectorTree queryTree = new ConnectorTree();
        queryTree.setOi(mainView.getOi());
        queryTree.setFields(queryField);
        return queryTree;
    }

    public static ConnectorTree buildConnectorTree(OIView mainView, Connector connector) {
        ConnectorTree queryTree = buildConnectorTree(mainView);
        queryTree.setConnector(connector);
        return queryTree;
    }
}
