package com.greatbee.core.manager.data;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.manager.utils.DataSourceUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by usagizhang on 17/12/21.
 */
public abstract class DataManagerTest extends DBBaseTest implements ExceptionCode {

    protected DS dataSource;
    protected DSView dsView;
    protected DSManager dsManager;

    protected RelationalDataManager dataManager;


    public DataManagerTest() {

    }

    public void setUp() throws DBException {
        super.setUp("test_server.xml");
        dsManager = (DSManager) context.getBean("dsManager");
        dataManager = this.getDataManager();
        this.initConn();
    }

    public void initConn() throws DBException {
        DSView dsView = getDSView();
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            //初始化数据库连接
            DS ds = dsView.getDs();
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            this.initSchema(conn, ps);
        } catch (Exception e) {
            e.printStackTrace();
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
     * 测试导出数据源
     *
     * @return
     */
    public DSView getDSView() {

        try {
            this.dataSource = this.getDS();
            DSView dv = this.getDsView();
            System.out.println("DSView -> " + JSONObject.toJSONString(dv));
            return dv;
        } catch (DBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public abstract void initSchema(Connection conn, PreparedStatement ps) throws DBException, SQLException;

    public abstract RelationalDataManager getDataManager();

    public abstract DS getDS() throws DBException;

    public abstract DSView getDsView() throws DBException;


}
