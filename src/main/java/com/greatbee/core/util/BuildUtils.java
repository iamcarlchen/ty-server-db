package com.greatbee.core.util;

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
 * connectorTree的解析
 * BuildUtils
 *
 * @author xiaobc
 * @date 17/7/20
 */
public class BuildUtils {

    private static final String BUILD_SELECT_FIELD = "select_field";
    private static final String BUILD_CONNECTOR = "connector";
    private static final String BUILD_CONDITION = "condition";
    private static final String BUILD_ORDER_BY = "order_by";
    private static final String BUILD_GROUP_BY = "group_by";

    private static final String BETWEEN_CONNECTOR_CONDITION_TYPE = "AND";

    /**
     * 构建全部sql 语句   group by 在 order by 前面
     * @param cont
     * @return
     */
    public static String buildAllSql(ConnectorTree cont){
        StringBuilder sql = new StringBuilder();
        sql.append(buildSelectFields(cont));
        sql.append(buildConnector(cont));
        sql.append(buildCondition(cont));
        sql.append(buildGroupBy(cont));
        sql.append(buildOrderBy(cont));
        return sql.toString();
    }

    /**
     * 构建select 字段 string
     *
     * @param cont
     * @return
     */
    public static String buildSelectFields(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree(BUILD_SELECT_FIELD, sql, null, cont);
        return sql.toString();
    }

    /**
     * 构建链接条件
     *
     * @param cont
     * @return
     */
    public static String buildConnector(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree(BUILD_CONNECTOR, sql, null, cont);
        return sql.toString();
    }

    /**
     * 构建condition条件
     *
     * @param cont
     * @return
     */
    public static String buildCondition(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree(BUILD_CONDITION, sql, null, cont);
        return StringUtil.isValid(sql.toString())?" WHERE "+sql.toString():"";
    }

    /**
     * 构建order by 语句
     *
     * @param cont
     * @return
     */
    public static String buildOrderBy(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree(BUILD_ORDER_BY, sql, null, cont);
        return sql.toString();
    }

    /**
     * 构建group by 语句
     *
     * @param cont
     * @return
     */
    public static String buildGroupBy(ConnectorTree cont) {
        StringBuilder sql = new StringBuilder();
        _buildTree(BUILD_GROUP_BY, sql, null, cont);
        return sql.toString();
    }

    /**
     * 构建方法  递归
     *
     * @param type
     * @param sql
     * @param prevCont    前一个cont
     * @param currentCont 当前的cont
     */
    private static void _buildTree(String type, StringBuilder sql, ConnectorTree prevCont, ConnectorTree currentCont) {
        if (BUILD_SELECT_FIELD.equals(type)) {
            Map<String,Field> fields = currentCont.getFields();
            if (CollectionUtil.isValid(fields)) {
                Iterator<Map.Entry<String, Field>> iter =  fields.entrySet().iterator();
                while (iter.hasNext()){
                    Map.Entry<String,Field> item = iter.next();
                    Field field = item.getValue();
                    String fieldAlias = item.getKey();
                    if (sql.length() == 0) {
                        if(field instanceof FunctionField){
                            sql.append(" SELECT ").append(((FunctionField) field).getFn()).append("(").append(currentCont.getConnectorId()).append(".`").append(((FunctionField) field).getParams()).append("`)");
                        }else {
                            sql.append(" SELECT ").append(currentCont.getConnectorId()).append(".`").append(field.getFieldName()).append("`");
                        }
                    } else {
                        if(field instanceof FunctionField){
                            sql.append(",").append(((FunctionField) field).getFn()).append("(").append(currentCont.getConnectorId()).append(".`").append(((FunctionField) field).getParams()).append("`)");
                        }else {
                            sql.append(",").append(currentCont.getConnectorId()).append(".`").append(field.getFieldName()).append("`");
                        }
                    }
                    sql.append(" AS `").append(fieldAlias).append("`");
                }
            }
        } else if (BUILD_CONNECTOR.equals(type)) {
            if (prevCont == null) {
                //root
                sql.append(" FROM `").append(currentCont.getOi().getResource()).append("`").append(" AS ").append(currentCont.getConnectorId()).append(" ");
            } else {
                //非root
                Connector connector = currentCont.getConnector();
                sql.append(ConT.getSqlJoinType(currentCont.getConT()));
                sql.append("`").append(currentCont.getOi().getResource()).append("`").append(" AS ").append(currentCont.getConnectorId()).append(" ");
                sql.append(" ON ").append(prevCont.getConnectorId()).append(".`").append(connector.getFromFieldName()).append("`=");
                sql.append(currentCont.getConnectorId()).append(".`").append(connector.getToFieldName()).append("` ");
            }
        } else if (BUILD_CONDITION.equals(type)) {
            if (currentCont.getCondition() != null) {
                if (sql.length() != 0) {
                    sql.append(" ").append(BETWEEN_CONNECTOR_CONDITION_TYPE).append(" ");
                }
                Condition.buildConditionSql(sql, currentCont.getCondition(), currentCont.getConnectorId());
            }
        } else if (BUILD_ORDER_BY.equals(type)) {
            OrderBy ob = currentCont.getOrderBy();
            if (ob != null) {
                if (sql.length() == 0) {
                    sql.append(" ORDER BY ");
                } else {
                    sql.append(",");
                }
                if(ob instanceof MultiOrderBy){
                    //多个字段排序
                    List<OrderBy> obs = ((MultiOrderBy) ob).getOrderBys();
                    for(int k=0;k<obs.size();k++){
                        if(k>0){
                            sql.append(",");
                        }
                        OrderBy _ob=obs.get(k);
                        if(noNeedConnecotrId(currentCont.getFields(),_ob.getOrderFieldName())){
                            sql.append(currentCont.getConnectorId()).append(".");
                        }
                        sql.append("`").append(_ob.getOrderFieldName()).append("` ").append(_ob.getOrder().getType());
                    }
                }else{
                    if(noNeedConnecotrId(currentCont.getFields(),ob.getOrderFieldName())){
                        sql.append(currentCont.getConnectorId()).append(".");
                    }
                    sql.append("`").append(ob.getOrderFieldName()).append("` ").append(ob.getOrder().getType());
                }
            }
        } else if (BUILD_GROUP_BY.equals(type)) {
            GroupBy gb = currentCont.getGroupBy();
            if (gb != null) {
                if (sql.length() == 0) {
                    sql.append(" GROUP BY ");
                } else {
                    sql.append(",");
                }
                //group by 支持多个字段分组，目前只支持单表
                String gbName = gb.getGroupFieldName();
                if(gbName.contains(",")){
                    String[] items =gbName.split(",");
                    for(int i=0;i<items.length;i++){
                        if(i>0){
                            sql.append(",");
                        }
                        sql.append(currentCont.getConnectorId()).append(".`").append(items[i]).append("`");
                    }
                }else{
                    sql.append(currentCont.getConnectorId()).append(".`").append(gb.getGroupFieldName()).append("`");
                }
            }
        }
        if (CollectionUtil.isValid(currentCont.getChildren())) {
            //有子集
            for (ConnectorTree childTree : currentCont.getChildren()) {
                _buildTree(type, sql, currentCont, childTree);
            }
        }
    }

    /**
     * 如果 排序的是函数字段的话，不需要加connectorId  true 为需要
     * @param fields
     * @param fieldName
     * @return
     */
    private static boolean noNeedConnecotrId(Map<String, Field>  fields,String fieldName){
        boolean need = true;
        if(StringUtil.isValid(fieldName)&&CollectionUtil.isValid(fields)){
            if(fields.containsKey(fieldName)&& (fields.get(fieldName) instanceof FunctionField)){
                need = false;
            }
        }
        return need;
    }

    /**
     * 构建方法参数  递归  构建condition条件 条件值，防注入
     *
     * @param index
     * @param ps
     * @param currentCont
     */
    public static int buildTreeConditionPs(int index, PreparedStatement ps, ConnectorTree currentCont) throws SQLException {
        if (currentCont.getCondition() != null) {
            index = Condition.buildConditionSqlPs(index, ps, currentCont.getCondition());
        }
        if (CollectionUtil.isValid(currentCont.getChildren())) {
            //有子集
            for (ConnectorTree childTree : currentCont.getChildren()) {
                index = buildTreeConditionPs(index, ps, childTree);
            }
        }
        return index;
    }

    /**
     * 构建rs 需要的字段map
     * @param cont
     * @param map
     */
    public static final void buildAllFields(ConnectorTree cont,Map<String,Field> map){
        if(CollectionUtil.isValid(cont.getFields())){
           map.putAll(cont.getFields());
        }
        if (CollectionUtil.isValid(cont.getChildren())) {
            //有子集
            for (ConnectorTree childTree : cont.getChildren()) {
                buildAllFields(childTree,map);
            }
        }
    }

    /**
     * 校验connectorTree 是否含有group by 查询
     * @param cont
     * @return
     */
    public static final boolean checkGroupBy(ConnectorTree cont){
        //是否有group by 的组数计算,如果是有group by 总数需要再套一层
        if(cont.getGroupBy()!=null&&StringUtil.isValid(cont.getGroupBy().getGroupFieldName())){
            return true;
        }
        boolean hasGroupBy = false;
        if (CollectionUtil.isValid(cont.getChildren())) {
            //有子集
            for (ConnectorTree childTree : cont.getChildren()) {
                hasGroupBy = hasGroupBy || checkGroupBy(childTree);
            }
        }
        return hasGroupBy;
    }


}
