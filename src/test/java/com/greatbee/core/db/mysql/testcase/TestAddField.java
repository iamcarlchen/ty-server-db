package com.greatbee.core.db.mysql.testcase;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.mysql.MysqlSchemaDataManagerTest;

import org.junit.Test;

public class TestAddField extends MysqlSchemaDataManagerTest implements ExceptionCode {

    public TestAddField() {
        this.setUp();
        this.init();
    }

    @Test
    public void testAddFieldByDefault() throws DBException {

    }

    /**
     * 初始化测试数据
     */
    public void init() {
        try {
            this.initConn();
            this.executeQuery(this.conn, this.ps, "DROP TABLE IF EXISTS `tb_test_add_field`;");
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("CREATE TABLE `tb_test_add_field` (");
            queryBuilder.append("`id` int(11) NOT NULL AUTO_INCREMENT,");
            queryBuilder.append("`remark` varchar(256) DEFAULT NULL,");
            queryBuilder.append("`alias` varchar(64) DEFAULT NULL,");
            queryBuilder.append("`name` varchar(64) DEFAULT NULL,");
            queryBuilder.append("PRIMARY KEY (`id`),");
            queryBuilder.append("UNIQUE KEY `id_UNIQUE` (`id`)");
            queryBuilder.append(") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
            this.executeQuery(this.conn, this.ps, queryBuilder.toString());
            this.releaseConn();

        } catch (DBException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}