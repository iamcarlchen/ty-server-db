package com.greatbee.core.db.mysql.manager;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.base.util.CollectionUtil;
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

import com.greatbee.core.bean.view.DiffItem;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.db.RelationalDataManager;
import com.greatbee.core.db.SchemaDataManager;
import com.greatbee.core.db.mysql.util.MysqlSchemaUtil;

import com.greatbee.core.manager.DSManager;
import com.greatbee.core.util.BuildUtils;
import com.greatbee.core.util.DataSourceUtils;
import com.greatbee.core.util.OIUtils;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Mysql Data Manager
 * <p>
 * Created by CarlChen on 2016/12/19.
 */
public class MysqlDataManager implements RelationalDataManager, SchemaDataManager, ExceptionCode {

    private static Logger logger = Logger.getLogger(MysqlDataManager.class);

    /**
     * dsManager 直接链接nvwa配置库,主要用于获取connection
     */
    @Autowired
    private DSManager dsManager;

    public Data read(OI oi, List<Field> fields, Field pkField) throws DBException {

        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("SELECT ");
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (i > 0) {
                    sql.append(" , ");
                }
                String fieldName = field.getFieldName();
                sql.append(" `").append(fieldName).append("` ");
            }
            sql.append(" FROM `").append(oi.getResource()).append("` ");
            // SQL Value用?处理
            sql.append(" WHERE `").append(pkField.getFieldName()).append("`=? ");

            logger.info("读取对象SQL：" + sql.toString());

            ps = conn.prepareStatement(sql.toString());
            _setPsParamPk(1, ps, pkField);
            rs = ps.executeQuery();

            Data map = new Data();
            while (rs.next()) {
                for (Field _f : fields) {
                    String _dt = _f.getDt();
                    if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getBoolean(_f.getFieldName()));
                    } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getDouble(_f.getFieldName()));
                    } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getInt(_f.getFieldName()));
                    } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Timestamp ts = rs.getTimestamp(_f.getFieldName());
                        map.put(_f.getFieldName(), ts == null ? "" : (sdf.format(ts)));
                    } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getDate(_f.getFieldName()));
                    } else {
                        map.put(_f.getFieldName(), rs.getString(_f.getFieldName()));
                    }
                }
            }
            return map;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
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
     * 分页查询 获取总记录条数
     *
     * @param oi
     * @param condition
     * @return
     */
    private int _pageTotalCount(OI oi, Condition condition) throws DBException {
        int result = 0;
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("SELECT count(*) ");

            sql.append(" FROM `").append(oi.getResource()).append("` ");
            if (condition != null) {
                sql.append(" WHERE ");
                Condition.buildConditionSql(sql, condition);
            }

            logger.info("查询对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString());//返回主键
            int index = Condition.buildConditionSqlPs(1, ps, condition);//前面没有？参数，所以从1开始,条件后面也可以再添加参数，索引从index开始
            rs = ps.executeQuery();

            while (rs.next()) {
                result = rs.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
                }
            }
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
        return result;
    }

    public DataPage page(OI oi, List<Field> fields, int page, int pageSize, Condition condition) throws DBException {

        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("SELECT ");
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (i > 0) {
                    sql.append(" , ");
                }
                String fieldName = field.getFieldName();
                sql.append(" `").append(fieldName).append("` ");
            }
            sql.append(" FROM `").append(oi.getResource()).append("` ");
            if (condition != null) {
                sql.append(" WHERE ");
                Condition.buildConditionSql(sql, condition);
            }
            sql.append(" LIMIT ?,? ");
            logger.info("查询对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString());//返回主键
            int index = Condition.buildConditionSqlPs(1, ps, condition);//前面没有？参数，所以从1开始,条件后面也可以再添加参数，索引从index开始
            //分页参数
            ps.setInt(index, (page - 1) * pageSize);
            ps.setInt(index + 1, pageSize);

            rs = ps.executeQuery();

            int count = _pageTotalCount(oi, condition);
            System.out.println("总记录数：" + count);

            List<Data> list = new ArrayList<Data>();
            while (rs.next()) {
                Data map = new Data();
                for (Field _f : fields) {
                    String _dt = _f.getDt();
                    if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getBoolean(_f.getFieldName()));
                    } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getDouble(_f.getFieldName()));
                    } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getInt(_f.getFieldName()));
                    } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Timestamp ts = rs.getTimestamp(_f.getFieldName());
                        map.put(_f.getFieldName(), ts == null ? "" : (sdf.format(ts)));
                    } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getDate(_f.getFieldName()));
                    } else {
                        map.put(_f.getFieldName(), rs.getString(_f.getFieldName()));
                    }
                }
                list.add(map);
            }
            DataPage dp = new DataPage();
            dp.setCurrentPage(page);
            dp.setCurrentRecords(list);
            dp.setCurrentRecordsNum(list.size());
            dp.setPageSize(pageSize);
            dp.setTotalPages((count % pageSize > 0) ? (count / pageSize + 1) : count / pageSize);
            dp.setTotalRecords(count);
            return dp;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
                }
            }
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

    public DataList list(OI oi, List<Field> fields, Condition condition) throws DBException {

        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("SELECT ");
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (i > 0) {
                    sql.append(" , ");
                }
                String fieldName = field.getFieldName();
                sql.append(" `").append(fieldName).append("` ");
            }
            sql.append(" FROM `").append(oi.getResource()).append("` ");
            if (condition != null) {
                sql.append(" WHERE ");
                Condition.buildConditionSql(sql, condition);
            }

            logger.info("查询对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString());//返回主键
            int index = Condition.buildConditionSqlPs(1, ps, condition);//前面没有？参数，所以从1开始,条件后面也可以再添加参数，索引从index开始
            rs = ps.executeQuery();

            List<Data> list = new ArrayList<Data>();
            while (rs.next()) {
                Data map = new Data();
                for (Field _f : fields) {
                    String _dt = _f.getDt();
                    if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getBoolean(_f.getFieldName()));
                    } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getDouble(_f.getFieldName()));
                    } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getInt(_f.getFieldName()));
                    } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Timestamp ts = rs.getTimestamp(_f.getFieldName());
                        map.put(_f.getFieldName(), ts == null ? "" : (sdf.format(ts)));
                    } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
                        map.put(_f.getFieldName(), rs.getDate(_f.getFieldName()));
                    } else {
                        map.put(_f.getFieldName(), rs.getString(_f.getFieldName()));
                    }
                }
                list.add(map);
            }
            return new DataList(list);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
                }
            }
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

    public void delete(OI oi, Field pkField) throws DBException {
        // 删除SQL
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("DELETE FROM ");
            sql.append("`").append(oi.getResource()).append("` ");
            // SQL Value用?处理
            sql.append(" WHERE `").append(pkField.getFieldName()).append("`=? ");

            logger.info("删除对象SQL：" + sql.toString());

            ps = conn.prepareStatement(sql.toString());
            _setPsParamPk(1, ps, pkField);
            ps.executeUpdate();
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

    public void delete(OI oi, Condition condition) throws DBException {
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("DELETE FROM ");
            sql.append("`").append(oi.getResource()).append("` ");
            if (condition != null) {
                sql.append(" WHERE ");
                Condition.buildConditionSql(sql, condition);
            }

            logger.info("查询对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString());//返回主键
            int index = Condition.buildConditionSqlPs(1, ps, condition);//前面没有？参数，所以从1开始,条件后面也可以再添加参数，索引从index开始
            ps.executeUpdate();

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
     * 创建数据
     *
     * @param oi
     * @param fields
     * @return 返回创建数据的唯一ID
     */
    public String create(OI oi, List<Field> fields) throws DBException {
        // 通过OI,fields拼装SQL

        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("INSERT INTO ");
            sql.append(oi.getResource()).append("(");
            StringBuilder valueStr = new StringBuilder();
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (i > 0) {
                    sql.append(",");
                    valueStr.append(",");
                }
                sql.append("`").append(field.getFieldName()).append("`");
                valueStr.append(" ? ");
                _checkFieldLengthOverLimit(field);
            }
            sql.append(") VALUES(");
            sql.append(valueStr);
            sql.append(")");

            logger.info("创建对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);//返回主键
            _setPsParam(1, ps, fields);
            ps.executeUpdate();

            rs = ps.getGeneratedKeys();
            int id = 0;
            if (rs.next()) {
                id = rs.getInt(1);
            }
            return String.valueOf(id);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
                }
            }
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
     * fields 只要是fields里有的字段都会更新，包括主键，所以这里应该不能传主键或者不可修改的字段进来
     *
     * @param oi
     * @param fields
     * @param pkField
     */
    public void update(OI oi, List<Field> fields, Field pkField) throws DBException {
        // 更新SQL
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("UPDATE ");
            sql.append("`").append(oi.getResource()).append("` ");
            sql.append(" SET ");
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (i > 0) {
                    sql.append(",");
                }
                sql.append("`").append(field.getFieldName()).append("`=? ");
                _checkFieldLengthOverLimit(field);
            }
            sql.append(" WHERE `").append(pkField.getFieldName()).append("`=").append(pkField.getFieldValue());

            logger.info("更新对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString());//返回主键
            _setPsParam(1, ps, fields);
            ps.executeUpdate();

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

    public void update(OI oi, List<Field> fields, Condition condition) throws DBException {
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = _ds.getConnection();

            StringBuilder sql = new StringBuilder("UPDATE ");
            sql.append("`").append(oi.getResource()).append("` ");
            sql.append(" SET ");
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (i > 0) {
                    sql.append(",");
                }
                sql.append("`").append(field.getFieldName()).append("`=? ");
                _checkFieldLengthOverLimit(field);
            }
            if (condition != null) {
                sql.append(" WHERE ");
                Condition.buildConditionSql(sql, condition);
            }

            logger.info("更新对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString());//返回主键
            int _index = _setPsParam(1, ps, fields);
            Condition.buildConditionSqlPs(_index, ps, condition);//前面没有？参数，所以从1开始,条件后面也可以再添加参数，索引从index开始
            ps.executeUpdate();

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
     * 校验字段值是否超过字段长度限制
     *
     * @param field
     * @throws DBException
     */
    private static void _checkFieldLengthOverLimit(Field field) throws DBException {
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
     * 设置ps参数 多个条件   防注入
     *
     * @param index  默认从1开始
     * @param ps
     * @param fields
     * @throws SQLException
     */
    private static int _setPsParam(int index, PreparedStatement ps, List<Field> fields) throws SQLException {
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
    private static void _setPsParamPk(int index, PreparedStatement ps, Field field) throws SQLException {
        List<Field> fields = new ArrayList<Field>();
        fields.add(field);
        _setPsParam(index, ps, fields);
    }

    @Override
    public DSView exportFromPhysicsDS(DS ds) throws DBException {
        Connection conn = null;
        DSView dsView = new DSView();
        try {
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            DatabaseMetaData metaData = conn.getMetaData();

            ResultSet rs = metaData.getTables(null, null, "", new String[] { "TABLE" });
            dsView.setDs(ds);
            List<OIView> oiViews = new ArrayList<OIView>();
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
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
                    field.setDt(MysqlSchemaUtil.transferMysqlTypeToTySqlType(dataType, colSize));
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

                oiViews.add(oiView);
            }
            dsView.setOiViews(oiViews);
            return dsView;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
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
     * 从DSView生成MysqlSchema
     *
     * @param dsView
     */
    @Override
    public void importFromDS(DSView dsView) throws DBException {
        if (dsView != null && dsView.getOiViews() != null && dsView.getDs() != null) {
            DS ds = dsView.getDs();

            Connection conn = null;
            Statement ps = null;
            try {
                conn = DataSourceUtils.getDatasource(ds).getConnection();
                ps = conn.createStatement();
                for (OIView oiView : dsView.getOiViews()) {
                    List<Field> fields = oiView.getFields();
                    String tableName = oiView.getOi().getResource();
                    String sql = MysqlSchemaUtil._buildCreateTableSql(ds, tableName, fields);
                    System.out.println("create table sql=" + sql);
                    ps.addBatch(sql);
                }
                ps.executeBatch();//批量执行
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
            } catch (DBException e) {
                e.printStackTrace();
                throw new DBException(e.getMessage(), e.getCode());
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

    /**
     * drop data table  正在testcase的时候用到了
     * 删除数据表
     *
     * @param dsView
     * @throws DBException
     */
    public void dropTable(DSView dsView) throws DBException {
        if (dsView != null && dsView.getOiViews() != null && dsView.getDs() != null) {
            DS ds = dsView.getDs();
            Connection conn = null;
            Statement ps = null;
            try {
                conn = DataSourceUtils.getDatasource(ds).getConnection();
                ps = conn.createStatement();
                for (OIView oiView : dsView.getOiViews()) {
                    String tableName = oiView.getOi().getResource();
                    if (MysqlSchemaUtil.isTableExits(ds, tableName)) {
                        StringBuilder dropSQL = new StringBuilder();
                        dropSQL.append("DROP TABLE ");
                        dropSQL.append(tableName);
                        ps.addBatch(dropSQL.toString());
                    }
                }
                ps.executeBatch();//批量执行
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
            } catch (DBException e) {
                e.printStackTrace();
                throw new DBException(e.getMessage(), e.getCode());
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

    @Override
    public Data read(ConnectorTree connectorTree) throws DBException {
        DataList dl = this.list(connectorTree);
        List<Data> datas = dl.getList();
        if (CollectionUtil.isValid(datas)) {
            return datas.get(0);
        } else {
            return new Data();
        }
    }

    /**
     * 分页查询 获取总记录条数
     *
     * @param cont
     * @return
     * @throws DBException
     */
    @Override
    public int count(ConnectorTree cont) throws DBException {
        if (cont == null || cont.getOi() == null) {
            throw new DBException("查询条件无效", ExceptionCode.ERROR_DB_CONT_IS_NULL);
        }
        OI oi = cont.getOi();
        int result = 0;
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = _ds.getConnection();
            //是否有group by 的组数计算,如果是有group by 总数需要再套一层
            boolean hasGroupBy = BuildUtils.checkGroupBy(cont);
            StringBuilder sql = new StringBuilder("SELECT count(*) ");
            if (hasGroupBy) {
                sql.append(" FROM (SELECT count(*) ");
            }
            sql.append(BuildUtils.buildConnector(cont));
            sql.append(BuildUtils.buildCondition(cont));
            sql.append(BuildUtils.buildGroupBy(cont));
            if (hasGroupBy) {
                sql.append(" ) as tmpTb");
            }

            logger.info("查询对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString());//返回主键
            int index = BuildUtils.buildTreeConditionPs(1, ps, cont);
            rs = ps.executeQuery();

            while (rs.next()) {
                result = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
                }
            }
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
        return result;
    }

    @Override
    public DataPage page(int page, int pageSize, ConnectorTree connectorTree) throws DBException {
        if (connectorTree == null || connectorTree.getOi() == null) {
            throw new DBException("查询条件无效", ExceptionCode.ERROR_DB_CONT_IS_NULL);
        }
        OI oi = connectorTree.getOi();
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = _ds.getConnection();

            String sql = BuildUtils.buildAllSql(connectorTree);
            sql = sql + " LIMIT ?,? ";
            logger.info("查询对象SQL：" + sql);
            ps = conn.prepareStatement(sql);//返回主键
            int index = BuildUtils.buildTreeConditionPs(1, ps, connectorTree);
            //分页参数
            ps.setInt(index, (page - 1) * pageSize);
            ps.setInt(index + 1, pageSize);

            rs = ps.executeQuery();

            int count = count(connectorTree);
            System.out.println("总记录数：" + count);

            List<Data> list = new ArrayList<Data>();
            Map<String, Field> map = new HashMap<String, Field>();
            BuildUtils.buildAllFields(connectorTree, map);
            while (rs.next()) {
                Data item = new Data();
                for (Map.Entry<String, Field> entry : map.entrySet()) {
                    String _dt = DT.String.getType();
                    if (entry.getValue() != null) {
                        _dt = entry.getValue().getDt();
                    }
                    if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
                        item.put(entry.getKey(), rs.getBoolean(entry.getKey()));
                    } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
                        item.put(entry.getKey(), rs.getDouble(entry.getKey()));
                    } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
                        item.put(entry.getKey(), rs.getInt(entry.getKey()));
                    } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Timestamp ts = rs.getTimestamp(entry.getKey());
                        item.put(entry.getKey(), (ts == null ? "" : sdf.format(ts)));
                    } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
                        item.put(entry.getKey(), rs.getDate(entry.getKey()));
                    } else {
                        item.put(entry.getKey(), rs.getString(entry.getKey()));
                    }
                }
                list.add(item);
            }

            DataPage dp = new DataPage();
            dp.setCurrentPage(page);
            dp.setCurrentRecords(list);
            dp.setCurrentRecordsNum(list.size());
            dp.setPageSize(pageSize);
            dp.setTotalPages((count % pageSize > 0) ? (count / pageSize + 1) : count / pageSize);
            dp.setTotalRecords(count);
            return dp;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
                }
            }
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

    @Override
    public DataList list(ConnectorTree connectorTree) throws DBException {
        if (connectorTree == null || connectorTree.getOi() == null) {
            throw new DBException("查询条件无效", ExceptionCode.ERROR_DB_CONT_IS_NULL);
        }
        OI oi = connectorTree.getOi();
        DataSource _ds = DataSourceUtils.getDatasource(oi.getDsAlias(), dsManager);
        if (_ds == null) {
            throw new DBException("获取数据源失败", ExceptionCode.ERROR_DB_DS_NOT_FOUND);
        }
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = _ds.getConnection();
            String sql = BuildUtils.buildAllSql(connectorTree);
            logger.info("查询对象SQL：" + sql.toString());
            ps = conn.prepareStatement(sql.toString());//返回主键
            int index = BuildUtils.buildTreeConditionPs(1, ps, connectorTree);

            //            String tomcatJdbcPsSql = ps.toString().split(";")[2];
            //            String realSql = tomcatJdbcPsSql.substring(tomcatJdbcPsSql.indexOf(":")+1,tomcatJdbcPsSql.length()-1);
            //            JdbcTemplate jdbcTemplate = new JdbcTemplate();
            //            jdbcTemplate.setDataSource(_ds);
            //            List<Map<String, Object>> list = jdbcTemplate.queryForList(realSql);

            rs = ps.executeQuery();
            List<Data> list = new ArrayList<Data>();
            Map<String, Field> map = new HashMap<String, Field>();
            BuildUtils.buildAllFields(connectorTree, map);
            while (rs.next()) {
                Data item = new Data();

                for (Map.Entry<String, Field> entry : map.entrySet()) {
                    String _dt = DT.String.getType();
                    if (entry.getValue() != null) {
                        _dt = entry.getValue().getDt();
                    }
                    if (DT.Boolean.getType().equalsIgnoreCase(_dt)) {
                        item.put(entry.getKey(), rs.getBoolean(entry.getKey()));
                    } else if (DT.Double.getType().equalsIgnoreCase(_dt)) {
                        item.put(entry.getKey(), rs.getDouble(entry.getKey()));
                    } else if (DT.INT.getType().equalsIgnoreCase(_dt)) {
                        item.put(entry.getKey(), rs.getInt(entry.getKey()));
                    } else if (DT.Time.getType().equalsIgnoreCase(_dt)) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Timestamp ts = rs.getTimestamp(entry.getKey());
                        item.put(entry.getKey(), (ts == null ? "" : sdf.format(ts)));
                    } else if (DT.Date.getType().equalsIgnoreCase(_dt)) {
                        item.put(entry.getKey(), rs.getDate(entry.getKey()));
                    } else {
                        item.put(entry.getKey(), rs.getString(entry.getKey()));
                    }
                }
                list.add(item);
            }
            return new DataList(list);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ExceptionCode.ERROR_DB_SQL_EXCEPTION);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new DBException("关闭ResultSet错误", ExceptionCode.ERROR_DB_RS_CLOSE_ERROR);
                }
            }
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
     * 差异化比较
     */
    @Override
    public List<DiffItem> diff(DSView configDSView) throws DBException {
        DS ds = configDSView.getDs();
        List<OIView> configOIViews = configDSView.getOiViews();
        DSView dbDSView = this.exportFromPhysicsDS(ds);
        List<OIView> dbOIViews = dbDSView.getOiViews();

        boolean dbHaveSchema = CollectionUtil.isValid(dbOIViews);

        List<DiffItem> diffItemList = new ArrayList<DiffItem>();

        if (CollectionUtil.isValid(configOIViews)) {
            //配置表里有OI
            if (!dbHaveSchema) {
                //数据库中没有表结构，直接创建
                for (OIView oiView : configOIViews) {
                    //拉出oiView下面所有的字段
                    List<Field> oiFields = oiView.getFields();
                    if (CollectionUtil.isValid(oiFields)) {
                        for (Field field : oiFields) {
                            //生成diff元件
                            diffItemList.add(
                                    new DiffItem("2", oiView.getOi().getResource(), field.getFieldName(), null, field));
                        }
                    }
                    //没有字段就跳过
                }
            } else {
                //数据库中有表结构，需要比对

                //填装oi Map的数据
                for (OIView oiView : configOIViews) {
                    if (CollectionUtil.isValid(oiView.getFields())) {
                        //遍历db表结构
                        OIView existDBView = null;
                        for (OIView dbView : dbOIViews) {
                            if (dbView.getOi().getResource().equalsIgnoreCase(oiView.getOi().getResource())) {
                                //表结构存在
                                existDBView = dbView;
                            }
                        }
                        List<Field> oiFields = oiView.getFields();
                        if (existDBView == null) {
                            //表结构不存在
                            for (Field field : oiFields) {
                                //生成diff元件
                                diffItemList.add(new DiffItem("2", oiView.getOi().getResource(), field.getFieldName(),
                                        null, field));
                            }
                        }

                        for (Field oiField : oiFields) {
                            Field existDBField = null;
                            if (CollectionUtil.isValid(existDBView.getFields())) {
                                for (Field dbField : existDBView.getFields()) {
                                    if (dbField.getFieldName().equalsIgnoreCase(oiField.getFieldName())) {
                                        existDBField = dbField;
                                    }
                                }
                            }
                            if (existDBField == null) {
                                //字段不存在
                                diffItemList.add(new DiffItem("4", oiView.getOi().getResource(), oiField.getFieldName(),
                                        null, oiField));
                            } else {
                                //表和字段都存在，比对属性
                                // oiField
                                // existDBField
                                if (oiField.getFieldLength() != existDBField.getFieldLength()
                                        || !oiField.getDt().equalsIgnoreCase(existDBField.getDt())) {
                                    //字段长度或类型不一样的时候
                                    //生成diff元件
                                    diffItemList.add(new DiffItem("5", oiView.getOi().getResource(),
                                            oiField.getFieldName(), existDBField, oiField));
                                }
                            }
                        }
                    } else {
                        //这个oi没有配置字段
                    }
                }
                //填装db Map的数据
                for (OIView dbView : dbOIViews) {
                    if (CollectionUtil.isValid(dbView.getFields())) {
                        OIView existOiView = null;
                        for (OIView oiView : configOIViews) {
                            if (oiView.getOi().getResource().equalsIgnoreCase(dbView.getOi().getResource())) {
                                existOiView = oiView;
                            }
                        }
                        List<Field> dbFields = dbView.getFields();
                        if (existOiView == null) {
                            //oi中缺少物理库中存在的表
                            for (Field dbField : dbFields) {
                                diffItemList.add(new DiffItem("1", dbView.getOi().getResource(), dbField.getFieldName(),
                                        dbField, null));
                            }
                        } else {
                            //OI中和物理库中都存在此表，比对字段是否相互存在
                            Field existOIField = null;
                            for (Field dbField : dbFields) {
                                for (Field oiField : existOiView.getFields()) {
                                    if (oiField.getFieldName().equalsIgnoreCase(dbField.getFieldName())) {
                                        existOIField = oiField;
                                    }
                                }

                                if (existOIField == null) {
                                    //OI中缺少物理表中的字段
                                    diffItemList.add(new DiffItem("1", dbView.getOi().getResource(),
                                            dbField.getFieldName(), dbField, null));
                                }
                            }
                        }
                    } else {
                        //db这个表没有字段
                    }
                }

            }
        } else if (dbHaveSchema) {
            //配置表里没有OI，直接返回需要添加的OI
            for (OIView dbView : dbOIViews) {
                //拉出oiView下面所有的字段
                List<Field> dbFields = dbView.getFields();
                if (CollectionUtil.isValid(dbFields)) {
                    for (Field field : dbFields) {
                        //生成diff元件
                        diffItemList.add(
                                new DiffItem("1", dbView.getOi().getResource(), field.getFieldName(), field, null));
                    }
                }
                //没有字段就跳过
            }
        } else {
            //oi中没配置db中也没有schema
        }

        return diffItemList;
    }

    /**
     * 创建表
     * done!
     */
    @Override
    public void createTable(OI oi, List<Field> dFields) throws DBException {
        System.out.println("createTable->" + oi.getDsAlias());
        OIUtils.isValid(oi);
        //获取ds
        DS ds = dsManager.getDSByAlias(oi.getDsAlias());
        //执行创建表
        MysqlSchemaUtil.createTable(ds, oi.getResource(), dFields);
    }

    /**
     * 删除表
     * done！
     */
    @Override
    public void dropTable(OI oi) throws DBException {
        OIUtils.isValid(oi);
        DS ds = dsManager.getDSByAlias(oi.getDsAlias());
        MysqlSchemaUtil.dropTable(ds, oi.getResource());
    }

    /**
     * 添加字段
     * done!
     */
    @Override
    public void addField(OI oi, Field field) throws DBException {
        OIUtils.isValid(oi);
        //读取之前schema
        DS ds = dsManager.getDSByAlias(oi.getDsAlias());
        OIView oiView = MysqlSchemaUtil.dumpTable(ds, oi.getAlias());
        OIUtils.isViewValid(oiView);
        //判断字段名称是否重复
        if (OIUtils.hasViewField(oiView, field.getFieldName())) {
            throw new DBException("字段已存在", ExceptionCode.ERROR_DB_FIELD_EXIST);
        }
        //添加字段
        MysqlSchemaUtil.addField(ds, oi.getResource(), field);
    }

    /**
     *  删除字段
     *  done!
     */
    @Override
    public void dropField(OI oi, Field field) throws DBException {
        OIUtils.isValid(oi);
        //读取之前schema
        DS ds = dsManager.getDSByAlias(oi.getDsAlias());
        OIView oiView = MysqlSchemaUtil.dumpTable(ds, oi.getAlias());
        OIUtils.isViewValid(oiView);
        //判断字段名称是否存在
        if (!OIUtils.hasViewField(oiView, field.getFieldName())) {
            throw new DBException("字段不存在", ExceptionCode.ERROR_DB_FIELD_NOT_EXIST);
        }
        //删除字段
        MysqlSchemaUtil.dropTableField(ds, oi.getAlias(), field.getFieldName());
    }

    /**
     * 更新字段
     * done!
     */
    @Override
    public void updateField(OI oi, Field oldField, Field newField) throws DBException {
        OIUtils.isValid(oi);
        //读取之前schema
        DS ds = dsManager.getDSByAlias(oi.getDsAlias());
        OIView oiView = MysqlSchemaUtil.dumpTable(ds, oi.getResource());
        OIUtils.isViewValid(oiView);
        //判断字段名称是否存在
        if (!OIUtils.hasViewField(oiView, oldField.getFieldName())) {
            throw new DBException("字段不存在", ExceptionCode.ERROR_DB_FIELD_NOT_EXIST);
        }
        //更新字段
        MysqlSchemaUtil.updateField(ds, oi.getResource(), oldField, newField);
    }

    /**
     * 更新字段
     * done!
     */
    @Override
    public void updateField(OI oi, Field updatField) throws DBException {
        this.updateField(oi, updatField, updatField);
    }
}
