package com.greatbee.core.bean.oi;

import com.greatbee.base.bean.AliasBean;
import com.greatbee.base.bean.IND;
import com.greatbee.base.bean.ext.SimpleIND;
import com.greatbee.core.bean.constant.DST;

/**
 * DS(Datasource)
 * <p/>
 * <p/>
 * 需要支持以下数据源：
 * 0. 静态
 * 1. Mysql
 * 2. RestAPI
 * 3. Dubbo
 * 4. File
 *
 * @see DST
 * <p/>
 * Created by CarlChen on 16/10/11.
 */
public class DS extends SimpleIND implements IND, AliasBean {
    //别名
    private String alias;

    //数据源类型
    private String dst;
    //数据源配置来源   两种，一种是数据库，另一种是从配置文件中获取链接信息  sql/file
    private String dsConfigFrom;

    //链接URL /project/storage
    private String connectionUrl;
    //链接用户名
    private String connectionUsername;
    //链接密码
    private String connectionPassword;

    public String getAlias() {
        return alias;
    }

    public String getDst() {
        return dst;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getConnectionUsername() {
        return connectionUsername;
    }

    public String getConnectionPassword() {
        return connectionPassword;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public void setConnectionUsername(String connectionUsername) {
        this.connectionUsername = connectionUsername;
    }

    public void setConnectionPassword(String connectionPassword) {
        this.connectionPassword = connectionPassword;
    }

    public String getDsConfigFrom() {
        return dsConfigFrom;
    }

    public void setDsConfigFrom(String dsConfigFrom) {
        this.dsConfigFrom = dsConfigFrom;
    }
}
