package com.greatbee.core.manager.data.sqlserver.manager;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.DataUtil;
import com.greatbee.base.util.RandomGUIDUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.manager.data.RelationalDataManager;
import com.greatbee.core.manager.data.base.manager.DataManager;
import com.greatbee.core.manager.data.oracle.util.OracleBuildUtils;
import com.greatbee.core.manager.data.oracle.util.OracleConditionUtil;
import com.greatbee.core.manager.data.sqlserver.util.SqlServerBuildUtils;
import com.greatbee.core.manager.data.sqlserver.util.SqlServerConditionUtil;
import com.greatbee.core.manager.data.util.DataSourceUtils;
import com.greatbee.core.manager.data.util.LoggerUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * SQL Server Data Manager
 * <p>
 * Author: CarlChen
 * Date: 2017/11/18
 */
public class SQLServerDataManagerV2 extends DataManager {

    private LoggerUtil logger = new LoggerUtil(SQLServerDataManagerV2.class);

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
//            ResultSet rs = e.getTables((String) null, (String) null, "", new String[]{"TABLE"});
//            String schemaName = ds.getName().toUpperCase();
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
    public void buildingPageQuery(int page, int pageSize, ConnectorTree connectorTree, Connection conn, PreparedStatement ps) throws SQLException {
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
    }

    @Override
    public Data buildingDataObject(ResultSet rs, Map.Entry entry) throws SQLException {
        Data dp = new Data();
        String _dt = DT.String.getType();
        if (entry.getValue() != null) {
            _dt = ((Field) entry.getValue()).getDt();
        }
        if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
            dp.put(entry.getKey().toString(), Boolean.valueOf(rs.getBoolean((String) entry.getKey())));
        } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
            dp.put(entry.getKey().toString(), Double.valueOf(rs.getDouble((String) entry.getKey())));
        } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
            dp.put(entry.getKey().toString(), Integer.valueOf(rs.getInt((String) entry.getKey())));
        } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Timestamp ts = rs.getTimestamp((String) entry.getKey());
            dp.put(entry.getKey().toString(), ts == null ? "" : sdf.format(ts));
        } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
            dp.put(entry.getKey().toString(), rs.getDate((String) entry.getKey()));
        } else {
            dp.put(entry.getKey().toString(), rs.getString((String) entry.getKey()));
        }
        return dp;
    }

    @Override
    public void buildingListQuery(ConnectorTree connectorTree, Connection conn, PreparedStatement ps) throws SQLException {
        String buildAllSql = SqlServerBuildUtils.buildAllSql(connectorTree);
        logger.info("查询对象SQL：" + buildAllSql.toString());
        ps = conn.prepareStatement(buildAllSql.toString());
        SqlServerBuildUtils.buildTreeConditionPs(1, ps, connectorTree);
    }

    @Override
    public void buildingCountQuery(OI oi, Condition condition, Connection conn, PreparedStatement ps) {

    }

    @Override
    public void buildingCountQuery(ConnectorTree connectorTree, Connection conn, PreparedStatement ps) {
        StringBuilder sqlBuilder = new StringBuilder("SELECT count(*) ");
        sqlBuilder.append(OracleBuildUtils.buildConnector(connectorTree));
        sqlBuilder.append(OracleBuildUtils.buildCondition(connectorTree));
        sqlBuilder.append(OracleBuildUtils.buildGroupBy(connectorTree));
        logger.info("查询对象SQL：" + sqlBuilder.toString());
    }

    @Override
    public void buildingReadQuery(OI oi, List<Field> fields, Field pkField, Connection conn, PreparedStatement ps) {

    }

    @Override
    public void buildingListQuery(OI oi, List<Field> fields, Condition condition, Connection conn, PreparedStatement ps) {

    }

    @Override
    public void buildingPageQuery(OI oi, List<Field> fields, int page, int pageSize, Condition condition, Connection conn, PreparedStatement ps) {

    }

    @Override
    public Data buildingDataObject(ResultSet rs, List<Field> fields) throws SQLException {
        Data resultData=new Data();
        Iterator iterator = fields.iterator();
        while (true) {
            Field field = (Field) iterator.next();
            String _dt = field.getDt();
            if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
                resultData.put(field.getFieldName(), Boolean.valueOf(rs.getBoolean(field.getFieldName())));
            } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
                resultData.put(field.getFieldName(), Double.valueOf(rs.getDouble(field.getFieldName())));
            } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
                resultData.put(field.getFieldName(), Integer.valueOf(rs.getInt(field.getFieldName())));
            } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Timestamp ts = rs.getTimestamp(field.getFieldName());
                resultData.put(field.getFieldName(), ts == null ? "" : sdf.format(ts));
            } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
                resultData.put(field.getFieldName(), rs.getDate(field.getFieldName()));
            } else {
                resultData.put(field.getFieldName(), rs.getString(field.getFieldName()));
            }
            if (!iterator.hasNext()) {
                break;
            }
        }
        return resultData;
    }

    @Override
    public void buildingDeleteQuery(OI oi, Condition condition, Connection conn, PreparedStatement ps) {

    }

    @Override
    public String executeCreateQuery(OI oi, List<Field> fields, Connection conn, PreparedStatement ps) {
        return null;
    }

    @Override
    public void buildingDeleteQuery(OI oi, Field pkField, Connection conn, PreparedStatement ps) {

    }

    @Override
    public void buildingUpdateQuery(OI oi, List<Field> fields, Field pkField, Connection conn, PreparedStatement ps) {

    }

    @Override
    public void buildingUpdateQuery(OI oi, List<Field> fields, Condition condition, Connection conn, PreparedStatement ps) {

    }
}
