
package com.greatbee.core.db.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DST;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.SchemaDataManager;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.util.DataSourceUtils;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * 测试 Mysql Schema Data Manager
 */
public abstract class MysqlSchemaDataManagerTest extends MysqlDataManagerTest implements ExceptionCode {

    public SchemaDataManager mysqlDataManager;

    /**
     * setUp 设置测试用例
     */
    public void setUp() {
        super.setUp();
        //加载manager
        mysqlDataManager = (SchemaDataManager) context.getBean("mysqlDataManager");
    }
}