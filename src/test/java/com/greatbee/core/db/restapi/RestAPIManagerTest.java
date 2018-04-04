package com.greatbee.core.db.restapi;

import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DST;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.constant.RestApiFieldGroupType;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.rest.RestAPIManager;
import com.greatbee.core.db.restapi.testcase.URMSLoginTest;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.util.DataSourceUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by usagizhang on 18/3/28.
 */
public class RestAPIManagerTest extends DBBaseTest implements ExceptionCode {
    @Autowired
    private DSManager dsManager;

    @Autowired
    protected RestAPIManager restAPIManager;

    public Connection conn = null;
    public PreparedStatement ps = null;

    private String authToken = "";


    public void setUp() {
        super.setUp("test_server.xml");
        // super.setUp("ty_db_server.xml");
        //加载manager
        dsManager = (DSManager) context.getBean("dsManager");
        restAPIManager = (RestAPIManager) context.getBean("restAPIManager");
        initTestData();
    }

    /**
     * 准备测试数据
     */
    protected void initTestData() {
        try {
            this.initDS();
        } catch (DBException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化DS数据源
     */
    protected void initDS() throws DBException, SQLException {
        //打开ty的数据库连接
        DS ds = dsManager.getDSByAlias("db_ty");
        this.initConn(ds);
        //清理测试数据
        this.executeQuery(this.conn, this.ps, "delete from ty_ds where alias in ('test_rest_urms','test_rest_ty')");
        this.releaseConn();
        //创建测试用的ds
        dsManager.add(this.initRestDS("test_rest_urms", "http://api-employee.dev.rs.com"));
        dsManager.add(this.initRestDS("test_rest_ty", "http://longyan.dev.rs.com"));
    }


    public void initConn(DS ds) throws DBException {
        try {
            //初始化数据库连接
            this.conn = DataSourceUtils.getDatasource(ds).getConnection();
            this.ps = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected DS initRestDS(String alias, String host) {
        DS ds = new DS();
        ds.setConnectionPassword("");
        ds.setAlias(alias);
        ds.setName(alias);
        ds.setConnectionUrl(host);
        ds.setDsConfigFrom("sql");
        ds.setDst(DST.RestAPI.getType());
        ds.setConnectionUsername("");
        ds.setDescription("");
        return ds;
    }

    /**
     * 设置oi
     *
     * @param dsAlias
     * @param url
     * @param alias
     * @return
     */
    protected OI initOI(String dsAlias, String url, String alias) {
        OI oi = new OI();
        oi.setDsAlias(dsAlias);
        oi.setResource(url);
        oi.setAlias(alias);
        oi.setName(alias);
        return oi;
    }


    /**
     * 设置field
     *
     * @param dt
     * @param length
     * @param type
     * @param fieldname
     * @param fieldvalue
     * @return
     */
    protected Field initField(DT dt, int length, RestApiFieldGroupType type, String fieldname, String fieldvalue) {
        return this.initField(dt, length, type, fieldname, fieldvalue, fieldname);
    }

    /**
     * 设置field
     *
     * @param dt
     * @param length
     * @param type
     * @param fieldname
     * @param fieldvalue
     * @param alias
     * @return
     */
    protected Field initField(DT dt, int length, RestApiFieldGroupType type, String fieldname, String fieldvalue, String alias) {
        Field field = new Field();
        field.setDt(dt.getType());
        field.setFieldName(fieldname);
        field.setFieldValue(fieldvalue);
        field.setFieldLength(length);
        field.setGroup(type.getType());
        field.setName(alias);
        field.setPk(false);

        return field;
    }


    public void executeQuery(Connection conn, PreparedStatement ps, String querySql) throws SQLException {
        ps = conn.prepareStatement(querySql);
        ps.execute();
    }

    public void releaseConn() throws DBException {
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

    /**
     * URMS登录
     */
    protected void URMSLogin() {
        try {
            URMSLoginTest urmsLoginTest = new URMSLoginTest();
            this.setAuthToken(urmsLoginTest.login());
        } catch (DBException e) {
            e.printStackTrace();
        }
    }

    protected String getAuthToken() {
        return authToken;
    }

    protected void setAuthToken(String authToken) {
        this.authToken = authToken;
    }
}
