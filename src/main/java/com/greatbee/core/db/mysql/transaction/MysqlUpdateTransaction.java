package com.greatbee.core.db.mysql.transaction;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.constant.DBTT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.base.BaseTransactionTemplate;
import com.greatbee.core.bean.view.Condition;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by usagizhang on 18/3/13.
 */
public class MysqlUpdateTransaction extends BaseTransactionTemplate {
    private static Logger logger = Logger.getLogger(MysqlUpdateTransaction.class);

    private List<Field> fields;

    private Condition condition;
    private StringBuilder sql;
    private boolean isUpdateByPK = false;

    public MysqlUpdateTransaction(OI oi, List<Field> fields, Field pkField) throws DBException {
        this.setDbtt(DBTT.Update);
        this.fields = fields;
        sql = new StringBuilder("UPDATE ");
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

        logger.info("组装更新对象SQL：" + sql.toString());
        isUpdateByPK = true;
    }

    public MysqlUpdateTransaction(OI oi, List<Field> fields, Condition condition) throws DBException {
        this.setDbtt(DBTT.Update);
        this.fields = fields;
        this.condition = condition;

        sql = new StringBuilder("UPDATE ");
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
        logger.info("组装更新对象SQL：" + sql.toString());
    }

    /**
     * 执行事务
     *
     * @param conn
     * @throws Exception
     */
    @Override
    public void executeTransaction(Connection conn) throws SQLException {
        logger.info("执行更新对象SQL：" + sql.toString());
        /**
         * 创建ps对象
         */
        ps = conn.prepareStatement(sql.toString());
        /**
         * 设置参数
         */
        if (isUpdateByPK) {
            _setPsParam(1, ps, fields);
        } else {
            int _index = _setPsParam(1, ps, fields);
            Condition.buildConditionSqlPs(_index, ps, condition);//前面没有？参数，所以从1开始,条件后面也可以再添加参数，索引从index开始
        }
        /**
         * 执行更新
         */
        ps.executeUpdate();
    }
}
