package com.greatbee.core.db.sqlserver.manager;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.util.RandomGUIDUtil;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.*;
import com.greatbee.core.db.base.DataManager;
import com.greatbee.core.db.oracle.util.OracleBuildUtils;
import com.greatbee.core.db.oracle.util.OracleConditionUtil;
import com.greatbee.core.db.sqlserver.util.SqlServerBuildUtils;
import com.greatbee.core.db.sqlserver.util.SqlServerConditionUtil;
import com.greatbee.core.util.DataSourceUtils;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * SQL Server Data Manager
 * <p>
 * Author: CarlChen
 * Date: 2017/11/18
 */
public class SQLServerDataManager extends DataManager {

    private static Logger logger = Logger.getLogger(SQLServerDataManager.class);

    @Override
    public DSView exportFromPhysicsDS(DS ds) throws DBException {
        Connection conn = null;
        DSView dsView = new DSView();

        DSView tableName1;
        try {
            logger.info("get connection from " + JSONObject.toJSONString(ds));
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            DatabaseMetaData metaData = conn.getMetaData();
            String schemaName = ds.getConnectionUrl().split("=")[ds.getConnectionUrl().split("=").length - 1];
            logger.info("exportFromPhysicsDS from " + schemaName);
            ResultSet rs = metaData.getTables(schemaName, null, null, new String[]{"TABLE"});
            dsView.setDs(ds);
            ArrayList oiViews = new ArrayList();

            while (rs.next()) {
                System.out.println(rs.getString("TABLE_NAME"));
                String tableName = rs.getString("TABLE_NAME");
                OI objectIdentified = new OI();
                objectIdentified.setName(tableName);
                objectIdentified.setAlias(tableName);
                objectIdentified.setResource(tableName);
                objectIdentified.setDsAlias(ds.getAlias());
                ResultSet columns = metaData.getColumns(null, null, tableName, "%");
                ResultSet pkCols = metaData.getPrimaryKeys((String) null, (String) null, tableName);
                ArrayList pkColNames = new ArrayList();

                while (pkCols.next()) {
                    String fields = pkCols.getString("COLUMN_NAME");
                    pkColNames.add(fields);
                }

                ArrayList fields1 = new ArrayList();

                while (columns.next()) {
                    Field field = new Field();
                    String colName = columns.getString("COLUMN_NAME");
                    int colSize = columns.getInt("COLUMN_SIZE");
                    int dataType = columns.getInt("DATA_TYPE");

                    String remarks = columns.getString("REMARKS");
                    if (remarks == null || remarks.equals("")) {
                        remarks = colName;
                    }

                    field.setName(colName);
                    field.setDt(this._transferMysqlTypeToTySqlType(dataType, colSize));
                    field.setFieldName(colName);
                    field.setOiAlias(objectIdentified.getAlias());
                    field.setFieldLength(Integer.valueOf(colSize));
                    field.setDescription(remarks);
                    boolean isPk = false;
                    Iterator iterator = pkColNames.iterator();

                    while (iterator.hasNext()) {
                        String str = (String) iterator.next();
                        if (str != null && str.equals(colName)) {
                            isPk = true;
                            break;
                        }
                    }

                    field.setPk(isPk);
                    fields1.add(field);
                }

                OIView oiView1 = new OIView();
                oiView1.setOi(objectIdentified);
                oiView1.setFields(fields1);
                oiViews.add(oiView1);
            }

            dsView.setOiViews(oiViews);
            tableName1 = dsView;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
        } finally {
            this._releaseConn(conn);
        }

        return tableName1;
    }

    @Override
    public void importFromDS(DSView dsView) throws DBException {

    }

    @Override
    public PreparedStatement buildingPageQuery(int page, int pageSize, ConnectorTree connectorTree, Connection conn, PreparedStatement ps) throws SQLException {
        String querySN = RandomGUIDUtil.getGUID(RandomGUIDUtil.RANDOM_8).replace("-", "").replaceAll("\\d+", "");
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("WITH ").append(querySN).append(" AS (")
                .append(SqlServerBuildUtils.buildSelectFields(connectorTree)).append(",ROW_NUMBER() OVER(").append(SqlServerBuildUtils.buildOrderBy(connectorTree)).append(") AS RowNo ").append(SqlServerBuildUtils.buildConnector(connectorTree)).append(SqlServerBuildUtils.buildCondition(connectorTree))
                .append(") SELECT * FROM ").append(querySN).append(" WHERE RowNo BETWEEN ? AND ?;");

        logger.info("查询对象SQL：" + queryBuilder.toString());
        ps = conn.prepareStatement(queryBuilder.toString());
        int index = SqlServerBuildUtils.buildTreeConditionPs(1, ps, connectorTree);
        ps.setInt(index + 1, pageSize);
        ps.setInt(index, (page - 1) * pageSize);
        return ps;
    }

    @Override
    public void buildingDataObject(ResultSet rs, Map.Entry entry, Data data) throws SQLException {
//        Data dp = new Data();
        String _dt = DT.String.getType();
        if (entry.getValue() != null) {
            _dt = ((Field) entry.getValue()).getDt();
        }
        if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
            data.put(entry.getKey().toString(), Boolean.valueOf(rs.getBoolean((String) entry.getKey())));
        } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
            data.put(entry.getKey().toString(), Double.valueOf(rs.getDouble((String) entry.getKey())));
        } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
            data.put(entry.getKey().toString(), Integer.valueOf(rs.getInt((String) entry.getKey())));
        } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Timestamp ts = rs.getTimestamp((String) entry.getKey());
            data.put(entry.getKey().toString(), ts == null ? "" : sdf.format(ts));
        } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
            data.put(entry.getKey().toString(), rs.getDate((String) entry.getKey()));
        } else {
            data.put(entry.getKey().toString(), rs.getString((String) entry.getKey()));
        }
//        return dp;
    }

    @Override
    public PreparedStatement buildingListQuery(ConnectorTree connectorTree, Connection conn, PreparedStatement ps) throws SQLException {
        String buildAllSql = SqlServerBuildUtils.buildAllSql(connectorTree);
        logger.info("查询对象SQL：" + buildAllSql.toString());
        ps = conn.prepareStatement(buildAllSql.toString());
        SqlServerBuildUtils.buildTreeConditionPs(1, ps, connectorTree);
        return ps;
    }

    @Override
    public PreparedStatement buildingCountQuery(OI oi, Condition condition, Connection conn, PreparedStatement ps) throws SQLException {

        StringBuilder sqlBuilder = new StringBuilder("SELECT count(*) ");
        sqlBuilder.append(" FROM \"").append(oi.getResource()).append("\" ");
        if (condition != null) {
            sqlBuilder.append(" WHERE ");
            OracleConditionUtil.buildConditionSql(sqlBuilder, condition);
        }

        logger.info("查询对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString());
        Condition.buildConditionSqlPs(1, ps, condition);

        return ps;
    }

    @Override
    public PreparedStatement buildingCountQuery(ConnectorTree connectorTree, Connection conn, PreparedStatement ps) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("SELECT count(*) ");
        sqlBuilder.append(OracleBuildUtils.buildConnector(connectorTree));
        sqlBuilder.append(OracleBuildUtils.buildCondition(connectorTree));
        sqlBuilder.append(OracleBuildUtils.buildGroupBy(connectorTree));
        logger.info("查询对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString());
        SqlServerBuildUtils.buildTreeConditionPs(1, ps, connectorTree);
        return ps;
    }

    @Override
    public PreparedStatement buildingReadQuery(OI oi, List<Field> fields, Field pkField, Connection conn, PreparedStatement ps) throws SQLException {

        StringBuilder sqlBuilder = new StringBuilder("SELECT ");

        for (int map = 0; map < fields.size(); ++map) {
            Field field = (Field) fields.get(map);
            if (map > 0) {
                sqlBuilder.append(" , ");
            }

            String fieldName = field.getFieldName();
            sqlBuilder.append(" \"").append(fieldName).append("\" ");
        }

        sqlBuilder.append(" FROM \"").append(oi.getResource()).append("\" ");
        sqlBuilder.append(" WHERE \"").append(pkField.getFieldName()).append("\"=? ");
        logger.info("读取对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString());
        _setPsParamPk(1, ps, pkField);

        return ps;
    }

    @Override
    public PreparedStatement buildingListQuery(OI oi, List<Field> fields, Condition condition, Connection conn, PreparedStatement ps) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("SELECT ");
        for (int index = 0; index < fields.size(); ++index) {
            Field list = (Field) fields.get(index);
            if (index > 0) {
                sqlBuilder.append(" , ");
            }

            String map = list.getFieldName();
            sqlBuilder.append(" \"").append(map).append("\" ");
        }
        sqlBuilder.append(" FROM \"").append(oi.getResource()).append("\" ");
        if (condition != null) {
            sqlBuilder.append(" WHERE ");
            SqlServerConditionUtil.buildConditionSql(sqlBuilder, condition);
        }
        logger.info("查询对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString());
        SqlServerConditionUtil.buildConditionSqlPs(1, ps, condition);
        return ps;
    }

    @Override
    public PreparedStatement buildingPageQuery(OI oi, List<Field> fields, int page, int pageSize, Condition condition, Connection conn, PreparedStatement ps) throws SQLException {

        StringBuilder fieldBuilder = new StringBuilder(" SELECT ");
        int index;
        for (index = 0; index < fields.size(); ++index) {
            Field count = (Field) fields.get(index);
            if (index > 0) {
                fieldBuilder.append(" , ");
            }

            String list = count.getFieldName();
            fieldBuilder.append(" \"").append(list).append("\" ");
        }


        StringBuilder conditionBuilder = new StringBuilder();
        if (condition != null) {
            conditionBuilder.append(" WHERE ");
            SqlServerConditionUtil.buildConditionSql(conditionBuilder, condition);
        }

        StringBuilder orderBuilder = new StringBuilder();
        for (int k = 0; k < fields.size(); k++) {
            Field field = (Field) fields.get(k);
            if (field != null && field.isPk()) {
                orderBuilder.append(" \"").append(field.getFieldName()).append("\" ");
                orderBuilder.append(" ASC ,");
            }
        }
        if (orderBuilder.length() > 0) {
            orderBuilder.insert(0, " ORDER BY ");
            orderBuilder.deleteCharAt(orderBuilder.lastIndexOf(","));
        }


        String querySN = RandomGUIDUtil.getGUID(RandomGUIDUtil.RANDOM_8).replace("-", "").replaceAll("\\d+", "");
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("WITH ").append(querySN).append(" AS (")
//                .append(fieldBuilder).append(",ROW_NUMBER() OVER(").append(SqlServerBuildUtils.buildOrderBy(connectorTree)).append(") AS RowNo ").append(" FROM \"").append(oi.getResource()).append("\" ").append(conditionBuilder)
                .append(fieldBuilder).append(",ROW_NUMBER() OVER(").append(orderBuilder).append(") AS RowNo ").append(" FROM \"").append(oi.getResource()).append("\" ").append(conditionBuilder)
//                .append(fieldBuilder).append(",ROW_NUMBER() AS RowNo ").append(" FROM \"").append(oi.getResource()).append("\" ").append(conditionBuilder)
                .append(") SELECT * FROM ").append(querySN).append(" WHERE RowNo BETWEEN ? AND ?;");

        logger.info("查询对象SQL：" + queryBuilder.toString());
        ps = conn.prepareStatement(queryBuilder.toString());
        index = SqlServerConditionUtil.buildConditionSqlPs(1, ps, condition);
        ps.setInt(index + 1, pageSize);
        ps.setInt(index, (page - 1) * pageSize);

        return ps;
    }

    @Override
    public void buildingDataObject(ResultSet rs, List<Field> fields, Data data) throws SQLException {
//        Data resultData = new Data();
        Iterator iterator = fields.iterator();
        while (true) {
            Field field = (Field) iterator.next();
            String _dt = field.getDt();
            if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
                data.put(field.getFieldName(), Boolean.valueOf(rs.getBoolean(field.getFieldName())));
            } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
                data.put(field.getFieldName(), Double.valueOf(rs.getDouble(field.getFieldName())));
            } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
                data.put(field.getFieldName(), Integer.valueOf(rs.getInt(field.getFieldName())));
            } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Timestamp ts = rs.getTimestamp(field.getFieldName());
                data.put(field.getFieldName(), ts == null ? "" : sdf.format(ts));
            } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
                data.put(field.getFieldName(), rs.getDate(field.getFieldName()));
            } else {
                data.put(field.getFieldName(), rs.getString(field.getFieldName()));
            }
            logger.info("buildingDataObject->" + JSONObject.toJSONString(data));
            if (!iterator.hasNext()) {
                break;
            }
        }
//        return resultData;
    }

    @Override
    public PreparedStatement buildingDeleteQuery(OI oi, Condition condition, Connection conn, PreparedStatement ps) throws SQLException {

        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM \"");
        sqlBuilder.append(oi.getResource()).append("\" ");
        if (condition != null) {
            sqlBuilder.append(" WHERE ");
            SqlServerConditionUtil.buildConditionSql(sqlBuilder, condition);
        }

        logger.info("查询对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString());
        SqlServerConditionUtil.buildConditionSqlPs(1, ps, condition);

        return ps;
    }

    @Override
    public String executeCreateQuery(OI oi, List<Field> fields, Connection conn, PreparedStatement ps) throws SQLException, DBException {

        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO \"");
        sqlBuilder.append(oi.getResource()).append("\" (");
        StringBuilder valueStr = new StringBuilder();
        List<Field> createFields = new ArrayList<Field>();

        int id = 0;
        for (int i = 0; i < fields.size(); ++i) {
            Field field = (Field) fields.get(i);
//            if (i > 0) {
//                sqlBuilder.append(",");
//                valueStr.append(",");
//            }

            if (field.isPk() && (field.getDt().equalsIgnoreCase(DT.INT.getType()) || field.getDt().equalsIgnoreCase(DT.Double.getType()))) {

            } else {
                sqlBuilder.append("\"").append(field.getFieldName()).append("\"");
                valueStr.append(" ? ");
                createFields.add(field);
                sqlBuilder.append(",");
                valueStr.append(",");
            }
            _checkFieldLengthOverLimit(field);
        }
        sqlBuilder.deleteCharAt(sqlBuilder.lastIndexOf(","));
        valueStr.deleteCharAt(valueStr.lastIndexOf(","));


        sqlBuilder.append(") VALUES(");
        sqlBuilder.append(valueStr);
        sqlBuilder.append(")");
        logger.info("创建对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString(), 1);
        _setPsParam(1, ps, createFields);

        ps.executeUpdate();
        ResultSet rs = ps.getGeneratedKeys();
        id = 0;
        if (rs.next()) {
            id = rs.getInt(1);
        }

        return String.valueOf(id);
    }

    @Override
    public PreparedStatement buildingDeleteQuery(OI oi, Field pkField, Connection conn, PreparedStatement ps) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM \"");
        sqlBuilder.append(oi.getResource()).append("\" ");
        sqlBuilder.append(" WHERE \"").append(pkField.getFieldName()).append("\"=? ");
        logger.info("删除对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString());
        _setPsParamPk(1, ps, pkField);
        return ps;
    }

    @Override
    public PreparedStatement buildingUpdateQuery(OI oi, List<Field> fields, Field pkField, Connection conn, PreparedStatement ps) throws DBException, SQLException {
        StringBuilder sqlBuilder = new StringBuilder("UPDATE ");
        sqlBuilder.append("\"").append(oi.getResource()).append("\"").append(" ");
        sqlBuilder.append(" SET ");

        for (int i = 0; i < fields.size(); ++i) {
            Field field = (Field) fields.get(i);
            if (!field.isPk()) {
                sqlBuilder.append("\"").append(field.getFieldName()).append("\"").append("=? ");
                sqlBuilder.append(",");
                _checkFieldLengthOverLimit(field);
            }
        }
        sqlBuilder.deleteCharAt(sqlBuilder.lastIndexOf(","));

        sqlBuilder.append(" WHERE ").append("\"").append(pkField.getFieldName()).append("\"").append("=").append(pkField.getFieldValue());
        logger.info("更新对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString());
        _setPsParam(1, ps, fields);
        return ps;
    }

    @Override
    public PreparedStatement buildingUpdateQuery(OI oi, List<Field> fields, Condition condition, Connection conn, PreparedStatement ps) throws DBException, SQLException {

        StringBuilder sqlBuilder = new StringBuilder("UPDATE ");
        sqlBuilder.append("\"").append(oi.getResource()).append("\"").append(" ");
        sqlBuilder.append(" SET ");

        int _index;
        for (int i = 0; i < fields.size(); ++i) {
            Field field = (Field) fields.get(i);
            if (!field.isPk()) {
                sqlBuilder.append("\"").append(field.getFieldName()).append("\"").append("=? ");
                sqlBuilder.append(",");
                _checkFieldLengthOverLimit(field);
            }
        }
        sqlBuilder.deleteCharAt(sqlBuilder.lastIndexOf(","));

        if (condition != null) {
            sqlBuilder.append(" WHERE ");
            SqlServerConditionUtil.buildConditionSql(sqlBuilder, condition);
        }

        logger.info("更新对象SQL：" + sqlBuilder.toString());
        ps = conn.prepareStatement(sqlBuilder.toString());
        _index = _setPsParam(1, ps, fields);
        SqlServerConditionUtil.buildConditionSqlPs(_index, ps, condition);

        return ps;
    }
}
