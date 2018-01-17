package com.greatbee.core.bean.view;

import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.RandomGUIDUtil;
import com.greatbee.core.bean.constant.ConT;
import com.greatbee.core.bean.oi.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Connector Tree
 * <p/>
 * Author :CarlChen
 * Date:17/7/19
 */
public class ConnectorTree {
    private Connector connector;
    private ConT conT = ConT.Left;
    private String connectorId;

    private OI oi;
    private Map<String, Field> fields;
    private MultiCondition condition;
    private GroupBy groupBy;
    private OrderBy orderBy;

    private List<ConnectorTree> children;

    /**
     * 获取子ConnectorTree
     *
     * @param connector connector
     * @return ConnectorTree
     */
    public ConnectorTree getChildConnectorTree(Connector connector, OI oi) {
        if (children == null) {
            children = new ArrayList<ConnectorTree>();
        }
        ConnectorTree target = null;
        for (ConnectorTree child : children) {
            if (child.getConnector().getId() == connector.getId()) {
                target = child;
            }
        }
        if (target == null) {
            target = new ConnectorTree();
            target.setConnector(connector);
            target.setOi(oi);
            children.add(target);
        }
        return target;
    }

    public String getConnectorId() {
        if (connectorId == null && connector != null) {
            connectorId = connector.getAlias() + RandomGUIDUtil.getGUID(RandomGUIDUtil.RANDOM_4);
        } else if (connector == null && oi != null) {
            //root tree
            connectorId = oi.getAlias();
        }
        return connectorId;
    }

    /**
     * Add Field
     *
     * @param field field
     */
    public void addField(String alias, Field field) {
        if (fields == null) {
            fields = new HashMap<String, Field>();
        }
        fields.put(alias, field);
    }

    /**
     * Add Condition
     *
     * @param c
     */
    public void addCondition(Condition c) {
        if (condition == null) {
            condition = new MultiCondition();
        }

        condition.addCondition(c);
    }

    public ConT getConT() {
        return conT;
    }

    public void setConT(ConT conT) {
        this.conT = conT;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public OI getOi() {
        return oi;
    }

    public void setOi(OI oi) {
        this.oi = oi;
    }

    public Map<String, Field> getFields() {
        return fields;
    }

    public void setFields(Map<String, Field> fields) {
        this.fields = fields;
    }

    public MultiCondition getCondition() {
        return condition;
    }

    public void setCondition(MultiCondition condition) {
        this.condition = condition;
    }

    public List<ConnectorTree> getChildren() {
        return children;
    }

    public void setChildren(List<ConnectorTree> children) {
        this.children = children;
    }

    public GroupBy getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(GroupBy groupBy) {
        this.groupBy = groupBy;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }
}
