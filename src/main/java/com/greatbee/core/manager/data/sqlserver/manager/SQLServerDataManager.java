package com.greatbee.core.manager.data.sqlserver.manager;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.base.util.DataUtil;
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
import com.greatbee.core.manager.data.oracle.util.OracleBuildUtils;
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
public class SQLServerDataManager implements RelationalDataManager, ExceptionCode {

    private LoggerUtil logger = new LoggerUtil(SQLServerDataManager.class);

    /**
     * dsManager 直接链接nvwa配置库,主要用于获取connection
     */
    @Autowired
    private DSManager dsManager;

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
                    field.setDt(_transferMysqlTypeToTySqlType(dataType, colSize));
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
    public Data read(ConnectorTree connectorTree) throws DBException {
        return null;
    }

    @Override
    public DataPage page(int page, int pageSize, ConnectorTree connectorTree) throws DBException {
        if (connectorTree != null && connectorTree.getOi() != null) {
            OI oi = connectorTree.getOi();
            DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), this.dsManager);
            if (_ds == null) {
                throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
            } else {
                Connection conn = null;
                PreparedStatement ps = null;
                ResultSet rs = null;

                DataPage result;
                try {
                    conn = _ds.getConnection();
                    String buildAllSql = "SELECT * FROM (SELECT \"TY_TABLE\".*, ROWNUM \"TY_ROWNUM\" FROM (" + OracleBuildUtils.buildAllSql(connectorTree);
                    buildAllSql = buildAllSql + " ) \"TY_TABLE\" WHERE ROWNUM <= ?) WHERE \"TY_ROWNUM\" > ? ";
                    logger.info("查询对象SQL：" + buildAllSql);
                    ps = conn.prepareStatement(buildAllSql);
                    int index = SqlServerBuildUtils.buildTreeConditionPs(1, ps, connectorTree);

                    ps.setInt(index, pageSize);
                    ps.setInt(index + 1, (page - 1) * pageSize);

                    rs = ps.executeQuery();
                    int count = this.count(connectorTree);
                    System.out.println("总记录数：" + count);
                    ArrayList list = new ArrayList();
                    HashMap map = new HashMap();
                    SqlServerBuildUtils.buildAllFields(connectorTree, map);

                    while (rs.next()) {
                        Data dp = new Data();
                        Iterator iterator = map.entrySet().iterator();

                        while (iterator.hasNext()) {
                            Map.Entry entry = (Map.Entry) iterator.next();
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
                        }

                        list.add(dp);
                    }

                    DataPage dataPage = new DataPage();
                    dataPage.setCurrentPage(page);
                    dataPage.setCurrentRecords(list);
                    dataPage.setCurrentRecordsNum(list.size());
                    dataPage.setPageSize(pageSize);
                    dataPage.setTotalPages(count % pageSize > 0 ? count / pageSize + 1 : count / pageSize);
                    dataPage.setTotalRecords(count);
                    result = dataPage;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
                } finally {
                    _releaseRs(rs);
                    _releaseConn(conn, ps);

                }

                return result;
            }
        } else {
            throw new DBException("查询条件无效", ERROR_DB_CONT_IS_NULL);
        }
    }

    @Override
    public DataList list(ConnectorTree connectorTree) throws DBException {
        if (connectorTree != null && connectorTree.getOi() != null) {
            OI oi = connectorTree.getOi();
            DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), this.dsManager);
            if (_ds == null) {
                throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
            } else {
                Connection conn = null;
                PreparedStatement ps = null;
                ResultSet rs = null;

                try {
                    conn = _ds.getConnection();
                    String buildAllSql = SqlServerBuildUtils.buildAllSql(connectorTree);
                    logger.info("查询对象SQL：" + buildAllSql.toString());
                    ps = conn.prepareStatement(buildAllSql.toString());
                    SqlServerBuildUtils.buildTreeConditionPs(1, ps, connectorTree);
                    rs = ps.executeQuery();
                    ArrayList list = new ArrayList();
                    HashMap map = new HashMap();
                    SqlServerBuildUtils.buildAllFields(connectorTree, map);

                    while (rs.next()) {
                        Data item = new Data();
                        Iterator iterator = map.entrySet().iterator();

                        while (iterator.hasNext()) {
                            Map.Entry entry = (Map.Entry) iterator.next();
                            String _dt = DT.String.getType();
                            if (entry.getValue() != null) {
                                _dt = ((Field) entry.getValue()).getDt();
                            }

                            if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
                                item.put(entry.getKey().toString(), Boolean.valueOf(rs.getBoolean((String) entry.getKey())));
                            } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
                                item.put(entry.getKey().toString(), Double.valueOf(rs.getDouble((String) entry.getKey())));
                            } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
                                item.put(entry.getKey().toString(), Integer.valueOf(rs.getInt((String) entry.getKey())));
                            } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                Timestamp ts = rs.getTimestamp((String) entry.getKey());
                                item.put(entry.getKey().toString(), ts == null ? "" : sdf.format(ts));
                            } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
                                item.put(entry.getKey().toString(), rs.getDate((String) entry.getKey()));
                            } else {
                                item.put(entry.getKey().toString(), rs.getString((String) entry.getKey()));
                            }
                        }

                        list.add(item);
                    }

                    DataList item1 = new DataList(list);
                    return item1;
                } catch (SQLException exception) {
                    exception.printStackTrace();
                    throw new DBException(exception.getMessage(), ERROR_DB_SQL_EXCEPTION);
                } finally {
                    _releaseRs(rs);
                    _releaseConn(conn, ps);

                }
            }
        } else {
            throw new DBException("查询条件无效", ERROR_DB_CONT_IS_NULL);
        }
    }

    @Override
    public int count(ConnectorTree connectorTree) throws DBException {
        return 0;
    }

    //    @Override
    public void connect(OI oi, List<Field> list) throws DBException {

    }

    @Override
    public Data read(OI oi, List<Field> fields, Field pkField) throws DBException {
        return null;
    }

    @Override
    public DataPage page(OI oi, List<Field> fields, int page, int pageSize, Condition condition) throws DBException {
        return null;
    }

    @Override
    public DataList list(OI oi, List<Field> fields, Condition condition) throws DBException {
        return null;
    }

    @Override
    public void delete(OI oi, Field pkField) throws DBException {

    }

    @Override
    public void delete(OI oi, Condition condition) throws DBException {

    }

    @Override
    public String create(OI oi, List<Field> fields) throws DBException {
        return null;
    }

    @Override
    public void update(OI oi, List<Field> fields, Field pkField) throws DBException {

    }

    @Override
    public void update(OI oi, List<Field> fields, Condition condition) throws DBException {

    }


    private void _releaseConn(Connection conn, PreparedStatement ps) throws DBException {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭PreparedStatement错误", ERROR_DB_PS_CLOSE_ERROR);
            }
        }

        this._releaseConn(conn);
    }

    private void _releaseConn(Connection conn) throws DBException {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭connection错误", ERROR_DB_CONN_CLOSE_ERROR);
            }
        }
    }

    private void _releaseRs(ResultSet rs) throws DBException {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭ResultSet错误", ERROR_DB_RS_CLOSE_ERROR);
            }
        }
    }

    private int _pageTotalCount(OI oi, Condition condition) throws DBException {
        int result = 0;
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), this.dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
        } else {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;

            try {
                conn = _ds.getConnection();
                StringBuilder sqlBuilder = new StringBuilder("SELECT count(*) ");
                sqlBuilder.append(" FROM \"").append(oi.getResource()).append("\" ");
                if (condition != null) {
                    sqlBuilder.append(" WHERE ");
                    SqlServerConditionUtil.buildConditionSql(sqlBuilder, condition);
                }

                logger.info("查询对象SQL：" + sqlBuilder.toString());
                ps = conn.prepareStatement(sqlBuilder.toString());
                Condition.buildConditionSqlPs(1, ps, condition);

                for (rs = ps.executeQuery(); rs.next(); result = rs.getInt(1)) {
                    ;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
            } finally {
                _releaseRs(rs);
                _releaseConn(conn, ps);
            }

            return result;
        }
    }


    private static void _checkFieldLengthOverLimit(Field field) throws DBException {
        if (!DT.Time.getType().equals(field.getDt()) && !DT.Date.getType().equals(field.getDt()) && !DT.Double.getType().equals(field.getDt()) && !DT.INT.getType().equals(field.getDt()) && (!DT.Boolean.getType().equals(field.getDt()) || field.getFieldValue() != null && !field.getFieldValue().equals("false") && !field.getFieldValue().equals("true"))) {
            if (StringUtil.isValid(field.getFieldValue()) && field.getFieldLength().intValue() > 0 && field.getFieldValue().length() > field.getFieldLength().intValue()) {
                throw new DBException("字段值长度超过字段限制长度", ERROR_DB_FIELD_LENGTH_OVER_LIMIT);
            }
        }
    }

    private static int _setPsParam(int index, PreparedStatement ps, List<Field> fields) throws SQLException {
        for (int i = 0; i < fields.size(); ++i) {
            Field f = (Field) fields.get(i);
            if (DT.INT.getType().equals(f.getDt())) {
                ps.setInt(i + index, DataUtil.getInt(f.getFieldValue(), 0));
            } else if (DT.Boolean.getType().equals(f.getDt())) {
                ps.setBoolean(i + index, DataUtil.getBoolean(f.getFieldValue(), false));
            } else if (DT.Double.getType().equals(f.getDt())) {
                ps.setDouble(i + index, DataUtil.getDouble(f.getFieldValue(), 0.0D));
            } else if (DT.Date.getType().equals(f.getDt())) {
                if (StringUtil.isInvalid(f.getFieldValue())) {
                    ps.setNull(i + index, 91);
                } else {
                    ps.setString(i + index, f.getFieldValue());
                }
            } else if (DT.Time.getType().equals(f.getDt())) {
                if (StringUtil.isInvalid(f.getFieldValue())) {
                    ps.setNull(i + index, 92);
                } else {
                    ps.setString(i + index, f.getFieldValue());
                }
            } else {
                ps.setString(i + index, f.getFieldValue());
            }
        }

        return index + fields.size();
    }

    private static void _setPsParamPk(int index, PreparedStatement ps, Field field) throws SQLException {
        ArrayList fields = new ArrayList();
        fields.add(field);
        _setPsParam(index, ps, fields);
    }

    private static String _transferMysqlTypeToTySqlType(int type, int colSize) {
        return type != 4 && (type != -6 || colSize <= 4) && type != -7 && type != -5 ? (type != 8 && type != 3 ? (type == 16 || type == -6 && colSize <= 4 ? DT.Boolean.getType() : (type != 12 && type != -1 && type != -9 && type != -16 && type != 2005 && type != 1 ? (type == 91 ? DT.Date.getType() : (type != 92 && type != 93 ? (type == 2000 ? DT.Object.getType() : (type == 2003 ? DT.Array.getType() : "")) : DT.Time.getType())) : DT.String.getType())) : DT.Double.getType()) : DT.INT.getType();
    }
}
