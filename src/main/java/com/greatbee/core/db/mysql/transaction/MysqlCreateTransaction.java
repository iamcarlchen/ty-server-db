package com.greatbee.core.db.mysql.transaction;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.constant.DBTT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.transaction.BaseTransactionTemplate;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by usagizhang on 18/3/13.
 */
public class MysqlCreateTransaction extends BaseTransactionTemplate {
    private static Logger logger = Logger.getLogger(MysqlCreateTransaction.class);

    private List<Field> fields;
    private StringBuilder sql;

    public MysqlCreateTransaction(OI oi, List<Field> fields) throws DBException {
        this.fields = fields;
        this.setDbtt(DBTT.Create);

        sql = new StringBuilder("INSERT INTO ");
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
        logger.info("组装创建对象SQL：" + sql.toString());
    }


    @Override
    public void executeTransaction(Connection conn) throws SQLException {
        logger.info("执行创建对象SQL：" + sql.toString());
        ps = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);//返回主键
        _setPsParam(1, ps, fields);
        ps.executeUpdate();
        /**
         * 返回自增长key
         */
        rs = ps.getGeneratedKeys();
        int id = 0;
        if (rs.next()) {
            id = rs.getInt(1);
        }
    }
}
