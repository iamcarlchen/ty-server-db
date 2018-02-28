package com.greatbee.core.db.mysql.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.util.DataSourceUtils;

/**
 * Created by usagizhang on 18/1/22.
 */
public class MysqlSchemaUtil implements ExceptionCode {
    //默认字符串长度
    private static final int DEFULT_STRING_LENGTH = 64;
    private static final String DB_ENGINE = "InnoDB";
    private static final String DB_ENCODING = "utf8";

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
    public static OIView dumpTable(DS ds, DatabaseMetaData metaData, String tableName)
            throws DBException, SQLException {
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
     * 创建表
     */
    public static void createTable(DS ds, String tableName, List<Field> dbFields) throws DBException {
        if (isTableExits(ds, tableName)) {
            StringBuilder createSQL = new StringBuilder();
            //创建表语句构建
            createSQL.append(_buildCreateTableSql(ds, tableName, dbFields));
            _executeQuery(ds, createSQL.toString());
        } else {
            throw new DBException("表不存在", ERROR_DB_TABLE_NOT_EXIST);
        }
    }

    /**
     * 删除表
     *
     * @param ds        ds
     * @param tableName tableName
     * @throws DBException DBException
     */
    public static void dropTable(DS ds, String tableName) throws DBException {
        if (isTableExits(ds, tableName)) {
            StringBuilder dropSQL = new StringBuilder();
            dropSQL.append("DROP TABLE ");
            dropSQL.append(tableName);
            _executeQuery(ds, dropSQL.toString());
        } else {
            throw new DBException("表不存在", ERROR_DB_TABLE_NOT_EXIST);
        }
    }

    /**
     * 添加字段
     */
    public static void addField(DS ds, String tableName, Field field) throws DBException {
        if (!isTableExits(ds, tableName)) {
            //表不存在
        }
        StringBuilder queryBuilder = new StringBuilder();
        //TODO:构建sql
        // alter table table1 add transactor varchar(10) not Null;
        // alter table   table1 add id int unsigned not Null auto_increment primary key
        queryBuilder.append("alter table ").append(tableName).append(" add `").append(field.getFieldName()).append("` ")
                .append(_getFieldSQLType(field.getDt(), field.getFieldLength()));
        if (field.isPk()) {
            //主键的情况
            queryBuilder.append(" primary key ;");
        } else {
            queryBuilder.append(" ; ");
        }

        _executeQuery(ds, queryBuilder.toString());

    }

    /**
     * 更新字段
     */
    public static void updateField(DS ds, String tableName, Field oldField, Field newField) throws DBException {
        if (!isTableExits(ds, tableName)) {
            //表不存在
        }
        StringBuilder queryBuilder = new StringBuilder();
        //TODO:构建sql
        // alter table 表名称 change 字段名称 字段名称 字段类型 [是否允许非空];
        if (oldField.getFieldName().equalsIgnoreCase(newField.getFieldName())) {
            //字段名称不修改的情况
            queryBuilder.append("alter table `").append(tableName).append("` change `").append(newField.getFieldName())
                    .append("` ").append(_getFieldSQLType(newField.getDt(), newField.getFieldLength()));
        } else {
            //字段名称修改的情况
            queryBuilder.append("alter table `").append(tableName).append("` change `").append(oldField.getFieldName())
                    .append("` `").append(newField.getFieldName()).append("` ")
                    .append(_getFieldSQLType(newField.getDt(), newField.getFieldLength()));
        }

        // if (field.isPk()) {
        //     //主键的情况
        //     queryBuilder.append(" primary key ;");
        // } else {
        //     queryBuilder.append(" ; ");
        // }
        queryBuilder.append(" ; ");

        _executeQuery(ds, queryBuilder.toString());

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
        StringBuilder dropSQL = new StringBuilder();
        dropSQL.append("alter table ").append(tableName).append(" drop column ").append(fieldName).append(";");
        _executeQuery(ds, dropSQL.toString());
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
        if (type == Types.INTEGER || (type == Types.TINYINT && colSize > 4) || type == Types.BIT
                || type == Types.BIGINT) {
            return DT.INT.getType();
        } else if (type == Types.DOUBLE || type == Types.DECIMAL) {
            return DT.Double.getType();
        } else if (type == Types.BOOLEAN || (type == Types.TINYINT && colSize <= 4)) {
            return DT.Boolean.getType();
        } else if (type == Types.VARCHAR || type == Types.LONGVARCHAR || type == Types.NVARCHAR
                || type == Types.LONGNVARCHAR || type == Types.CLOB || type == Types.CHAR) {
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

    /**
     * 组建建表sql
     *
     * @param tableName
     * @param dbFields
     * @throws DBException
     */
    public static String _buildCreateTableSql(DS ds, String tableName, List<Field> dbFields) throws DBException {
        if (tableName != null) {
            if (!MysqlSchemaUtil.isTableExits(ds, tableName)) {
                //只有表不存在时，才创建
                Field pkField = null;//标记主键Field
                StringBuilder sql = new StringBuilder();
                sql.append("CREATE TABLE `").append(tableName).append("`").append(" ( ");

                if (CollectionUtil.isValid(dbFields)) {
                    for (Field field : dbFields) {
                        String fieldName = field.getFieldName();
                        String dt = field.getDt();
                        if (field.isPk()) {
                            pkField = field;
                        }
                        if (field.isPk() && DT.INT.getType().equals(field.getDt())) {
                            //主键 如果是整形的话 需要自增长
                            sql.append("`").append(fieldName).append("`").append(" INT(11) NOT NULL AUTO_INCREMENT ")
                                    .append(",");
                        } else {
                            sql.append("`").append(fieldName).append("`")
                                    .append(_getFieldSQLType(dt, field.getFieldLength())).append(",");
                        }
                    }
                }
                sql.append("PRIMARY KEY (`").append(pkField.getFieldName()).append("`) ,").append("");
                sql.append("UNIQUE INDEX `").append(pkField.getFieldName()).append("_UNIQUE` (`")
                        .append(pkField.getFieldName()).append("` ASC) ").append("");
                sql.append(" ) ").append(" ENGINE=" + DB_ENGINE + " DEFAULT CHARSET=" + DB_ENCODING + ";");
                return sql.toString();
            }
            //如果存在表就直接跳过
        } else {
            throw new DBException("OI表名必须有效", ExceptionCode.ERROR_DB_OI_TABLE_NAME_INVAlID);
        }
        return null;
    }

    public static final String _getFieldSQLType(String dt, int length) {
        StringBuilder sql = new StringBuilder();
        if (DT.String.getType().equals(dt)) {
            if (length > 500) {
                sql.append(" TEXT DEFAULT NULL ");
            } else {
                sql.append(" VARCHAR(");
                if (length > 0) {
                    sql.append(length);
                } else {
                    sql.append(DEFULT_STRING_LENGTH);
                }
                sql.append(") DEFAULT NULL ");
            }
        } else if (DT.Boolean.getType().equals(dt)) {
            sql.append(" TINYINT(4) DEFAULT 0 ");
        } else if (DT.Date.getType().equals(dt)) {
            sql.append(" DATETIME DEFAULT NULL ");

        } else if (DT.Time.getType().equals(dt)) {
            sql.append(" DATETIME DEFAULT NULL ");
        } else if (DT.Double.getType().equals(dt)) {
            sql.append(" DECIMAL(10,2) DEFAULT 0 ");
        } else if (DT.INT.getType().equals(dt)) {
            sql.append(" INT(11) DEFAULT 0 ");
        }
        return sql.toString();
    }

    /**
     * 执行sql
     */
    private static void _executeQuery(DS ds, String sql) throws DBException {
        Connection conn = null;
        Statement ps = null;
        try {
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            ps = conn.createStatement();
            ps.addBatch(sql);
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
}
