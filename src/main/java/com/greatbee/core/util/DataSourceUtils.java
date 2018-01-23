package com.greatbee.core.util;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.DataUtil;
import com.greatbee.core.bean.constant.DSCF;
import com.greatbee.core.bean.constant.DST;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.manager.DSManager;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * DataSourceUtils
 *
 * @author xiaobc
 * @date 17/7/11
 */
public class DataSourceUtils {

    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    public static final String Oracle_Driver = "oracle.jdbc.OracleDriver";

    public static final String Sqlserver_Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static final String DB_CONFIG_PATH = "db/";

    private static final String DB_CONFIG_URL = "db.connection";
    private static final String DB_CONFIG_USERNAME = "db.connection.username";
    private static final String DB_CONFIG_PASSWORD = "db.connection.password";


    private static final String DB_CONFIG_MAXTOTAL = "db.config.maxTotal";
    private static final String DB_CONFIG_INITIALSIZE = "db.config.initialSize";
    private static final String DB_CONFIG_MINIDLE = "db.config.minIdle";
    private static final String DB_CONFIG_MAXIDLE = "db.config.maxIdle";


    // ds放到缓存中
    private static Map<String, DataSource> dataSourceConfigs = new HashMap<String, DataSource>();

    /**
     * 针对只有oi的情况
     *
     * @param dsAlias
     * @param dsManager
     * @return
     */
    public static DataSource getDatasource(String dsAlias, DSManager dsManager) {
        DS ds = null;
        try {
            if (dataSourceConfigs.containsKey(dsAlias)) {
                return dataSourceConfigs.get(dsAlias);
            } else {
                ds = dsManager.getDSByAlias(dsAlias);
            }
        } catch (DBException e) {
            //TODO Excepton
            e.printStackTrace();
        }
        return getDatasource(ds);
    }

    /**
     * 针对只有ds的情况
     *
     * @param ds
     * @return
     */
    public static DataSource getDatasource(DS ds) {
        DataSource __ds = null;
        if (dataSourceConfigs.containsKey(ds.getAlias())) {
            __ds = dataSourceConfigs.get(ds.getAlias());
        } else {
//            DriverManagerDataSource _ds = new DriverManagerDataSource();
//            BasicDataSource _ds = new BasicDataSource();
            //换成tomcat-jdbc pool  多线程速度快   dbcp是单线程的
            DataSource _ds = new DataSource();

            if (DST.Mysql.getType().equals(ds.getDst())) {
                _ds.setDriverClassName(MYSQL_DRIVER);
            }else if(DST.Oracle.getType().equals(ds.getDst())){
                _ds.setDriverClassName(Oracle_Driver);
            } else if (DST.SqlServer.getType().equals(ds.getDst())) {
                _ds.setDriverClassName(Sqlserver_Driver);
            }

            int maxTotal = 60;
            int initSize = 10;
            int minIdle = 8;
            int maxIdle = 16;
            if (DSCF.FILE.getType().equalsIgnoreCase(ds.getDsConfigFrom())) {
                //如果是从配置文件获取
                Properties pops= RedisBuildUtil.filterRedis(DB_CONFIG_PATH + ds.getAlias()+ ".properties");
                String url = pops.getProperty(DB_CONFIG_URL);
                String username = pops.getProperty(DB_CONFIG_USERNAME);
                String password = pops.getProperty(DB_CONFIG_PASSWORD);

                if(pops.containsKey(DB_CONFIG_MAXTOTAL)){
                    maxTotal = DataUtil.getInt(pops.getProperty(DB_CONFIG_MAXTOTAL),60);
                }
                if(pops.containsKey(DB_CONFIG_INITIALSIZE)){
                    initSize = DataUtil.getInt(pops.getProperty(DB_CONFIG_INITIALSIZE),10);
                }
                if(pops.containsKey(DB_CONFIG_MINIDLE)){
                    minIdle = DataUtil.getInt(pops.getProperty(DB_CONFIG_MINIDLE),8);
                }
                if(pops.containsKey(DB_CONFIG_MAXIDLE)){
                    maxIdle = DataUtil.getInt(pops.getProperty(DB_CONFIG_MAXIDLE),16);
                }

                _ds.setUrl(url);
                _ds.setUsername(username);
                _ds.setPassword(password);
            } else {
                _ds.setUrl(ds.getConnectionUrl());
                _ds.setUsername(ds.getConnectionUsername());
                _ds.setPassword(ds.getConnectionPassword());
            }

            ///设置空闲和借用的连接的最大总数量，同时可以激活。
//            _ds.setMaxTotal(maxTotal);
            _ds.setMaxActive(maxTotal);
            //设置初始大小
            _ds.setInitialSize(initSize);
            //最小空闲连接
            _ds.setMinIdle(minIdle);
            //最大空闲连接
            _ds.setMaxIdle(maxIdle);


            //超时等待时间毫秒
//            _ds.setMaxWaitMillis(3*10000);
            //只会发现当前连接失效，再创建一个连接供当前查询使用
            _ds.setTestOnBorrow(true);
            //removeAbandonedTimeout  ：超过时间限制，回收没有用(废弃)的连接（默认为 300秒，调整为180）
//            _ds.setRemoveAbandonedTimeout(300);
            //removeAbandoned  ：超过removeAbandonedTimeout时间后，是否进 行没用连接（废弃）的回收（默认为false，调整为true)
            //DATA_SOURCE.setRemoveAbandonedOnMaintenance(removeAbandonedOnMaintenance);
//            _ds.setRemoveAbandonedOnBorrow(true);
            //testWhileIdle
            _ds.setTestWhileIdle(true);
            //testOnReturn
            _ds.setTestOnReturn(true);
            //setRemoveAbandonedOnMaintenance
//            _ds.setRemoveAbandonedOnMaintenance(true);
            //记录日志
//            _ds.setLogAbandoned(true);
            //设置自动提交
            _ds.setDefaultAutoCommit(true);

            _ds.setValidationQuery("select 1 from dual");

            dataSourceConfigs.put(ds.getAlias(), _ds);

            __ds = _ds;
        }
        return __ds;
    }
}
