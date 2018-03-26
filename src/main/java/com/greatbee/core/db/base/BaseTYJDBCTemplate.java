package com.greatbee.core.db.base;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.DataUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.util.DataSourceUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 18/3/13.
 */
public class BaseTYJDBCTemplate implements ExceptionCode {

    /**
     * dsManager 直接链接nvwa配置库,主要用于获取connection
     */
    @Autowired
    protected DSManager dsManager;

    /**
     * 设置ps参数 多个条件   防注入
     *
     * @param index  默认从1开始
     * @param ps
     * @param fields
     * @throws SQLException
     */
    protected int _setPsParam(int index, PreparedStatement ps, List<Field> fields) throws SQLException {
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            if (DT.INT.getType().equals(f.getDt())) {
                ps.setInt(i + index, DataUtil.getInt(f.getFieldValue(), 0));
            } else if (DT.Boolean.getType().equals(f.getDt())) {
                ps.setBoolean(i + index, DataUtil.getBoolean(f.getFieldValue(), false));
            } else if (DT.Double.getType().equals(f.getDt())) {
                ps.setDouble(i + index, DataUtil.getDouble(f.getFieldValue(), 0));
            } else if (DT.Date.getType().equals(f.getDt())) {
                if (StringUtil.isInvalid(f.getFieldValue())) {
                    ps.setNull(i + index, Types.DATE);
                } else
                    ps.setString(i + index, f.getFieldValue());
            } else if (DT.Time.getType().equals(f.getDt())) {
                if (StringUtil.isInvalid(f.getFieldValue())) {
                    ps.setNull(i + index, Types.TIME);
                } else
                    ps.setString(i + index, f.getFieldValue());
            } else {
                ps.setString(i + index, f.getFieldValue());
            }
        }
        return index + fields.size();
    }


    /**
     * ps设置单个条件
     *
     * @param index
     * @param ps
     * @param field
     * @throws SQLException
     */
    protected void _setPsParamPk(int index, PreparedStatement ps, Field field) throws SQLException {
        List<Field> fields = new ArrayList<Field>();
        fields.add(field);
        _setPsParam(index, ps, fields);
    }

    /**
     * 校验字段值是否超过字段长度限制
     *
     * @param field
     * @throws DBException
     */
    protected void _checkFieldLengthOverLimit(Field field) throws DBException {
        if ((DT.INT.getType().equals(field.getDt())
                || (DT.Boolean.getType().equals(field.getDt())) && (field.getFieldValue() == null
                || field.getFieldValue().equals("false") || field.getFieldValue().equals("true")))) {
            //boolean类型直接过滤掉
            return;
        }
        if (StringUtil.isValid(field.getFieldValue()) && field.getFieldLength() > 0
                && (field.getFieldValue().length() > field.getFieldLength())) {
            throw new DBException("字段值长度超过字段限制长度", ExceptionCode.ERROR_DB_FIELD_LENGTH_OVER_LIMIT);
        }
    }

    /**
     * 根据DS的alias获取数据库连接
     *
     * @param dataSourceAlias
     * @return
     * @throws SQLException
     * @throws DBException
     */
    protected Connection getConnection(String dataSourceAlias) throws SQLException, DBException {
        DataSource _ds = DataSourceUtils.getDatasource(dataSourceAlias, this.dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ERROR_DB_DS_NOT_FOUND);
        }
        return _ds.getConnection();
    }


    /**
     * 释放ps对象
     *
     * @param ps
     * @throws DBException
     */
    protected void releasePreparedStatement(PreparedStatement ps) throws DBException {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭PreparedStatement错误", ExceptionCode.ERROR_DB_PS_CLOSE_ERROR);
            }
        }
    }

    protected void releasePreparedStatement(Statement ps) throws DBException {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭PreparedStatement错误", ExceptionCode.ERROR_DB_PS_CLOSE_ERROR);
            }
        }
    }

    /**
     * 释放rs对象
     *
     * @param rs
     * @throws DBException
     */
    protected void releaseResultSet(ResultSet rs) throws DBException {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
            }
        }
    }


    /**
     * 释放conn对象
     *
     * @param conn
     * @throws DBException
     */
    protected void releaseConnection(Connection conn) throws DBException {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭connection错误", ExceptionCode.ERROR_DB_CONN_CLOSE_ERROR);
            }
        }
    }
}
