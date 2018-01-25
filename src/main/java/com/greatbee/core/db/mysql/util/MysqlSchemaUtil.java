package com.greatbee.core.db.mysql.util;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.util.DataSourceUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 18/1/22.
 */
public class MysqlSchemaUtil implements ExceptionCode {

    public static void dumpTable() {

    }

    /**
     * dump 表
     * ds
     *
     * @param ds        ds
     * @param tableName tableName
     * @return OIView
     * @throws DBException DBException
     */
    public static OIView dumpTable(DS ds, String tableName) throws DBException {
        Connection conn = null;
        try {
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            DatabaseMetaData metaData = conn.getMetaData();
            return dumpTable(ds, metaData, tableName);
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


    /**
     * dump 表
     *
     * @param ds        ds
     * @param metaData  metaData
     * @param tableName tableName
     * @return OIView
     * @throws DBException  DBException
     * @throws SQLException SQLException
     */
    public static OIView dumpTable(DS ds, DatabaseMetaData metaData, String tableName) throws DBException, SQLException {
        OI oi = new OI();
        oi.setName(tableName);
        oi.setAlias(tableName);
        oi.setResource(tableName);
        oi.setDsAlias(ds.getAlias());

        ResultSet columns = metaData.getColumns(null, null, tableName, "");
        ResultSet pkCols = metaData.getPrimaryKeys(null, null, tableName);//获取主键列
        List<String> pkColNames = new ArrayList<String>();
        while (pkCols.next()) {
            String pkColName = pkCols.getString("COLUMN_NAME");
            pkColNames.add(pkColName);
        }
        List<Field> fields = new ArrayList<Field>();
        while (columns.next()) {
            Field field = new Field();
            String colName = columns.getString("COLUMN_NAME");
            int colSize = columns.getInt("COLUMN_SIZE");
            int dataType = columns.getInt("DATA_TYPE");//java.sql.Types
            String remarks = columns.getString("REMARKS");
            if (remarks == null || remarks.equals("")) {
                remarks = colName;
            }
            field.setName(colName);
            field.setDt(transferMysqlTypeToTySqlType(dataType, colSize));
            field.setFieldName(colName);
            field.setOiAlias(oi.getAlias());
            field.setFieldLength(colSize);
            field.setDescription(remarks);
            //是否主键
            boolean isPk = false;
            for (String str : pkColNames) {
                if (str != null && str.equals(colName)) {
                    isPk = true;
                    break;
                }
            }
            field.setPk(isPk);

            fields.add(field);
        }
        OIView oiView = new OIView();
        oiView.setOi(oi);
        oiView.setFields(fields);
        return oiView;
    }


    /**
     * 删除表
     *
     * @param ds        ds
     * @param tableName tableName
     * @throws DBException DBException
     */
    public static void dropTable(DS ds, String tableName) throws DBException {
        Connection conn = null;
        Statement ps = null;
        try {
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            ps = conn.createStatement();
            if (isTableExits(ds, tableName)) {
                StringBuilder dropSQL = new StringBuilder();
                dropSQL.append("DROP TABLE ");
                dropSQL.append(tableName);
                ps.addBatch(dropSQL.toString());
            } else {
                throw new DBException("表不存在", ERROR_DB_TABLE_NOT_EXIST);
            }
            ps.executeBatch();//批量执行
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
        } catch (DBException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), e.getCode());
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭PreparedStatement错误", ERROR_DB_PS_CLOSE_ERROR);
                }
            }
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

    /**
     * 删除表字段
     *
     * @param ds        ds
     * @param tableName tableName
     * @param fieldName fieldName
     * @throws DBException DBException
     */
    public static void dropTableField(DS ds, String tableName, String fieldName) throws DBException {
        Connection conn = null;
        Statement ps = null;
        try {
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            ps = conn.createStatement();
            StringBuilder dropSQL = new StringBuilder();
            dropSQL.append("alter table ").append(tableName).append(" drop column ").append(fieldName).append(";");
            ps.addBatch(dropSQL.toString());
            ps.executeBatch();//批量执行
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭PreparedStatement错误", ExceptionCode.ERROR_DB_PS_CLOSE_ERROR);
                }
            }
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

    /**
     * 校验表是否已经存在
     *
     * @param ds
     * @param tableName
     * @return
     * @throws DBException
     */
    public static boolean isTableExits(DS ds, String tableName) throws DBException {
        boolean isExist = false;
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            rs = conn.createStatement().executeQuery("show TABLES");
            while (rs.next()) {
                if (tableName.equalsIgnoreCase(rs.getString(1))) {
                    //找到存在的表
                    isExist = true;
                }
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException("show tables 异常:" + e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭connection错误", ExceptionCode.ERROR_DB_CONN_CLOSE_ERROR);
                }
            }
        }
        return isExist;
    }

    /**
     * java.sql.Types类型转换成DT类型
     *
     * @param type
     * @return
     * @throws Exception
     */
    public static String transferMysqlTypeToTySqlType(int type, int colSize) {
        if (type == Types.INTEGER || (type == Types.TINYINT && colSize > 4) || type == Types.BIT || type == Types.BIGINT) {
            return DT.INT.getType();
        } else if (type == Types.DOUBLE || type == Types.DECIMAL) {
            return DT.Double.getType();
        } else if (type == Types.BOOLEAN || (type == Types.TINYINT && colSize <= 4)) {
            return DT.Boolean.getType();
        } else if (type == Types.VARCHAR || type == Types.LONGVARCHAR || type == Types.NVARCHAR || type == Types.LONGNVARCHAR || type == Types.CLOB || type == Types.CHAR) {
            return DT.String.getType();
        } else if (type == Types.DATE) {
            return DT.Date.getType();
        } else if (type == Types.TIME || type == Types.TIMESTAMP) {
            return DT.Time.getType();
        } else if (type == Types.JAVA_OBJECT) {
            return DT.Object.getType();
        } else if (type == Types.ARRAY) {
            return DT.Array.getType();
        } else {
            return "";
        }
    }
}
