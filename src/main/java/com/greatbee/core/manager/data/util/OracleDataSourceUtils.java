package com.greatbee.core.manager.data.util;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.constant.DSCF;
import com.greatbee.core.bean.constant.DST;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.utils.RedisBuildUtil;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by usagizhang on 17/12/6.
 */
public class OracleDataSourceUtils {
    public static final String MYSQL_DRIVER = "oracle.jdbc.OracleDriver";
    private static final String DB_CONFIG_PATH = "db/";
    private static final String DB_CONFIG_URL = "db.connection";
    private static final String DB_CONFIG_USERNAME = "db.connection.username";
    private static final String DB_CONFIG_PASSWORD = "db.connection.password";
    private static Map<String, DataSource> dataSourceConfigs = new HashMap();

    public OracleDataSourceUtils() {
    }

    public static DataSource getDatasource(String dsAlias, DSManager dsManager) {
        DS ds = null;

        try {
            ds = dsManager.getDSByAlias(dsAlias);
        } catch (DBException e) {
            e.printStackTrace();
        }

        return getDatasource(ds);
    }

    public static DataSource getDatasource(DS ds) {
        Object __ds = null;
        if(dataSourceConfigs.containsKey(ds.getAlias())) {
            __ds = (DataSource)dataSourceConfigs.get(ds.getAlias());
        } else {
            DriverManagerDataSource _ds = new DriverManagerDataSource();
            if(DST.Oracle.getType().equals(ds.getDst())) {
                _ds.setDriverClassName("oracle.jdbc.OracleDriver");
            }

            if(DSCF.FILE.getType().equalsIgnoreCase(ds.getDsConfigFrom())) {
                Properties pops = RedisBuildUtil.filterRedis("db/" + ds.getAlias() + ".properties");
                String url = pops.getProperty("db.connection");
                String username = pops.getProperty("db.connection.username");
                String password = pops.getProperty("db.connection.password");
                _ds.setUrl(url);
                _ds.setUsername(username);
                _ds.setPassword(password);
            } else {
                _ds.setUrl(ds.getConnectionUrl());
                _ds.setUsername(ds.getConnectionUsername());
                _ds.setPassword(ds.getConnectionPassword());
            }

            dataSourceConfigs.put(ds.getAlias(), _ds);
            __ds = _ds;
        }

        return (DataSource)__ds;
    }


}
