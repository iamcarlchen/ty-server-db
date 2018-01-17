package com.greatbee.core.manager.data.base.manager;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.base.util.DataUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.manager.data.base.manager.handler.DataHandler;
import com.greatbee.core.manager.data.base.manager.handler.QueryHandler;
import com.greatbee.core.manager.utils.DataSourceUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * SQL Server Data Manager
 * <p>
 * Author: xuechao.zhang
 * Date: 2017/11/18
 */
public abstract class DBManager implements ExceptionCode {

    private static Logger logger = Logger.getLogger(DBManager.class);

    /**
     * dsManager 直接链接nvwa配置库,主要用于获取connection
     */
    @Autowired
    private DSManager dsManager;

    protected DataList executeListQuery(String dataSourceAlias, QueryHandler queryHandler, DataHandler dataHandler) throws DBException {
        DataSource _ds = DataSourceUtils.getDatasource(dataSourceAlias, this.dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
        } else {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;

            try {
                conn = _ds.getConnection();
                ps = queryHandler.execute(conn, ps);
                rs = ps.executeQuery();
                ArrayList list = new ArrayList();


                while (rs.next()) {
                    Data item = new Data();
                    dataHandler.execute(rs, item);
                    list.add(item);
                }

                DataList dataList = new DataList(list);
                return dataList;
            } catch (SQLException exception) {
                exception.printStackTrace();
                throw new DBException(exception.getMessage(), ERROR_DB_SQL_EXCEPTION);
            } finally {
                _releaseRs(rs);
                _releaseConn(conn, ps);

            }
        }
    }

    protected DataPage executePageQuery(String dataSourceAlias, int page, int pageSize, int count, QueryHandler queryHandler, DataHandler dataHandler) throws DBException {
        DataSource _ds = DataSourceUtils.getDatasource(dataSourceAlias, this.dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
        } else {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;

            DataPage result = new DataPage();
            try {
                conn = _ds.getConnection();
                ps = queryHandler.execute(conn, ps);
                rs = ps.executeQuery();
                System.out.println("总记录数：" + count);
                ArrayList list = new ArrayList();
                while (rs.next()) {
                    Data data = new Data();
                    dataHandler.execute(rs, data);
                    list.add(data);
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
    }

    protected Data executeReadQuery(String dataSourceAlias, QueryHandler queryHandler, DataHandler dataHandler) throws DBException {
        DataSource _ds = DataSourceUtils.getDatasource(dataSourceAlias, this.dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
        } else {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;

            try {
                conn = _ds.getConnection();
                ps = queryHandler.execute(conn, ps);
                rs = ps.executeQuery();

                Data result = new Data();
                if (rs.next()) {
                    dataHandler.execute(rs, result);
                }
                return result;
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new DBException("关闭connection错误", ERROR_DB_CONN_CLOSE_ERROR);
                    }
                }
            }
        }
    }

    protected int executCountQuery(String dataSourceAlias, QueryHandler handler) throws DBException {
        int result = 0;
        DataSource _ds = DataSourceUtils.getDatasource(dataSourceAlias, this.dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
        } else {
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;

            try {
                conn = _ds.getConnection();
                ps = handler.execute(conn, ps);
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

    protected void executUpdateQuery(String dataSourceAlias, QueryHandler handler) throws DBException {
        DataSource _ds = DataSourceUtils.getDatasource(dataSourceAlias, this.dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
        } else {
            Connection conn = null;
            PreparedStatement ps = null;

            try {
                conn = _ds.getConnection();
                ps = handler.execute(conn, ps);
                ps.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
            } finally {
                _releaseConn(conn, ps);
            }
        }
    }

    protected void _releaseConn(Connection conn, PreparedStatement ps) throws DBException {
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

    protected void _releaseConn(Connection conn) throws DBException {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭connection错误", ERROR_DB_CONN_CLOSE_ERROR);
            }
        }
    }

    protected void _releaseRs(ResultSet rs) throws DBException {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭ResultSet错误", ERROR_DB_RS_CLOSE_ERROR);
            }
        }
    }

    protected static void _checkFieldLengthOverLimit(Field field) throws DBException {
        if (!DT.Time.getType().equals(field.getDt()) && !DT.Date.getType().equals(field.getDt()) && !DT.Double.getType().equals(field.getDt()) && !DT.INT.getType().equals(field.getDt()) && (!DT.Boolean.getType().equals(field.getDt()) || field.getFieldValue() != null && !field.getFieldValue().equals("false") && !field.getFieldValue().equals("true"))) {
            if (StringUtil.isValid(field.getFieldValue()) && field.getFieldLength().intValue() > 0 && field.getFieldValue().length() > field.getFieldLength().intValue()) {
                throw new DBException("字段值长度超过字段限制长度", ERROR_DB_FIELD_LENGTH_OVER_LIMIT);
            }
        }
    }

    protected static int _setPsParam(int index, PreparedStatement ps, List<Field> fields) throws SQLException {
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

    protected static void _setPsParamPk(int index, PreparedStatement ps, Field field) throws SQLException {
        ArrayList fields = new ArrayList();
        fields.add(field);
        _setPsParam(index, ps, fields);
    }

    protected static String _transferMysqlTypeToTySqlType(int type, int colSize) {
        return type != 4 && (type != -6 || colSize <= 4) && type != -7 && type != -5 ? (type != 8 && type != 3 ? (type == 16 || type == -6 && colSize <= 4 ? DT.Boolean.getType() : (type != 12 && type != -1 && type != -9 && type != -16 && type != 2005 && type != 1 ? (type == 91 ? DT.Date.getType() : (type != 92 && type != 93 ? (type == 2000 ? DT.Object.getType() : (type == 2003 ? DT.Array.getType() : "")) : DT.Time.getType())) : DT.String.getType())) : DT.Double.getType()) : DT.INT.getType();
    }
}
