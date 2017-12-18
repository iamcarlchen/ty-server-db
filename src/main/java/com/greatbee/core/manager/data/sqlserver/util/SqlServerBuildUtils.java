package com.greatbee.core.manager.data.sqlserver.util;

import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.bean.constant.ConT;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.FunctionField;
import com.greatbee.core.bean.view.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by usagizhang on 17/12/15.
 */
public class SqlServerBuildUtils {
    private static final String BUILD_SELECT_FIELD = "select_field";
    private static final String BUILD_CONNECTOR = "connector";
    private static final String BUILD_CONDITION = "condition";
    private static final String BUILD_ORDER_BY = "order_by";
    private static final String BUILD_GROUP_BY = "group_by";
    private static final String BETWEEN_CONNECTOR_CONDITION_TYPE = "AND";

    public SqlServerBuildUtils() {
    }

    public static String buildAllSql(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        sql.append(buildSelectFields(cont));
        sql.append(buildConnector(cont));
        sql.append(buildCondition(cont));
        sql.append(buildGroupBy(cont));
        sql.append(buildOrderBy(cont));
        return sql.toString();
    }

    public static String buildSelectFields(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree("select_field", sql, (ConnectorTree) null, cont);
        return sql.toString();
    }

    public static String buildConnector(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree("connector", sql, (ConnectorTree) null, cont);
        return sql.toString();
    }

    public static String buildCondition(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree("condition", sql, (ConnectorTree) null, cont);
        return StringUtil.isValid(sql.toString()) ? " WHERE " + sql.toString() : "";
    }

    public static String buildOrderBy(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree("order_by", sql, (ConnectorTree) null, cont);
        return sql.toString();
    }

    public static String buildGroupBy(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree("group_by", sql, (ConnectorTree) null, cont);
        return sql.toString();
    }

    private static void _buildTree(String type, StringBuilder sql, ConnectorTree prevCont, ConnectorTree currentCont) {
        if ("select_field".equals(type)) {
            Map gb = currentCont.getFields();

            String fieldAlias;
            if (CollectionUtil.isValid(gb)) {
                for (Iterator childTree = gb.entrySet().iterator(); childTree.hasNext(); sql.append(" AS \"").append(fieldAlias).append("\"")) {
                    Map.Entry items = (Map.Entry) childTree.next();
                    Field itemsValue = (Field) items.getValue();
                    fieldAlias = (String) items.getKey();
                    if (sql.length() == 0) {
                        if (itemsValue instanceof FunctionField) {
                            sql.append(" SELECT ").append(((FunctionField) itemsValue).getFn()).append("(").append(currentCont.getConnectorId()).append(".\"").append(((FunctionField) itemsValue).getParams()).append("\")");
                        } else {
                            sql.append(" SELECT ").append(currentCont.getConnectorId()).append(".\"").append(itemsValue.getFieldName()).append("\"");
                        }
                    } else if (itemsValue instanceof FunctionField) {
                        sql.append(",").append(((FunctionField) itemsValue).getFn()).append("(").append(currentCont.getConnectorId()).append(".\"").append(((FunctionField) itemsValue).getParams()).append("\")");
                    } else {
                        sql.append(",").append(currentCont.getConnectorId()).append(".\"").append(itemsValue.getFieldName()).append("\"");
                    }
                }
            }
        } else if ("connector".equals(type)) {
            if (prevCont == null) {
                sql.append(" FROM \"").append(currentCont.getOi().getResource()).append("\"").append(" ").append(currentCont.getConnectorId()).append(" ");
            } else {
                Connector currentContConnector = currentCont.getConnector();
                sql.append(ConT.getSqlJoinType(currentCont.getConT()));
                sql.append("\"").append(currentCont.getOi().getResource()).append("\"").append("   ").append(currentCont.getConnectorId()).append(" ");
                sql.append(" ON ").append(prevCont.getConnectorId()).append(".\"").append(currentContConnector.getFromFieldName()).append("\"=");
                sql.append(currentCont.getConnectorId()).append(".\"").append(currentContConnector.getToFieldName()).append("\" ");
            }
        } else if ("condition".equals(type)) {
            if (currentCont.getCondition() != null) {
                if (sql.length() != 0) {
                    sql.append(" ").append("AND").append(" ");
                }

                Condition.buildConditionSql(sql, currentCont.getCondition(), currentCont.getConnectorId());
            }
        } else if ("order_by".equals(type)) {
            OrderBy var10 = currentCont.getOrderBy();
            if (var10 != null) {
                if (sql.length() == 0) {
                    sql.append(" ORDER BY ");
                } else {
                    sql.append(",");
                }

                if (var10 instanceof MultiOrderBy) {
                    List var13 = ((MultiOrderBy) var10).getOrderBys();

                    for (int var16 = 0; var16 < var13.size(); ++var16) {
                        if (var16 > 0) {
                            sql.append(",");
                        }

                        OrderBy var18 = (OrderBy) var13.get(var16);
                        sql.append(currentCont.getConnectorId()).append(".\"").append(var18.getOrderFieldName()).append("\" ").append(var18.getOrder().getType());
                    }
                } else {
                    sql.append(currentCont.getConnectorId()).append(".\"").append(var10.getOrderFieldName()).append("\" ").append(var10.getOrder().getType());
                }
            }
        } else if ("group_by".equals(type)) {
            GroupBy treeGroupBy = currentCont.getGroupBy();
            if (treeGroupBy != null) {
                if (sql.length() == 0) {
                    sql.append(" GROUP BY ");
                } else {
                    sql.append(",");
                }

                String groupFieldNameString = treeGroupBy.getGroupFieldName();
                if (groupFieldNameString.contains(",")) {
                    String[] groupByFieldNameArray = groupFieldNameString.split(",");

                    for (int i = 0; i < groupByFieldNameArray.length; ++i) {
                        if (i > 0) {
                            sql.append(",");
                        }

                        sql.append(currentCont.getConnectorId()).append(".\"").append(groupByFieldNameArray[i]).append("\"");
                    }
                } else {
                    sql.append(currentCont.getConnectorId()).append(".\"").append(treeGroupBy.getGroupFieldName()).append("\"");
                }
            }
        }

        if (CollectionUtil.isValid(currentCont.getChildren())) {
            Iterator iterator = currentCont.getChildren().iterator();

            while (iterator.hasNext()) {
                ConnectorTree nextTree = (ConnectorTree) iterator.next();
                _buildTree(type, sql, currentCont, nextTree);
            }
        }

    }

    public static int buildTreeConditionPs(int index, PreparedStatement ps, ConnectorTree currentCont) throws SQLException {
        if (currentCont.getCondition() != null) {
            index = Condition.buildConditionSqlPs(index, ps, currentCont.getCondition());
        }

        ConnectorTree childTree;
        if (CollectionUtil.isValid(currentCont.getChildren())) {
            for (Iterator var3 = currentCont.getChildren().iterator(); var3.hasNext(); index = buildTreeConditionPs(index, ps, childTree)) {
                childTree = (ConnectorTree) var3.next();
            }
        }

        return index;
    }

    public static final void buildAllFields(ConnectorTree cont, Map<String, Field> map) {
        if (CollectionUtil.isValid(cont.getFields())) {
            map.putAll(cont.getFields());
        }

        if (CollectionUtil.isValid(cont.getChildren())) {
            Iterator var2 = cont.getChildren().iterator();

            while (var2.hasNext()) {
                ConnectorTree childTree = (ConnectorTree) var2.next();
                buildAllFields(childTree, map);
            }
        }

    }
}
