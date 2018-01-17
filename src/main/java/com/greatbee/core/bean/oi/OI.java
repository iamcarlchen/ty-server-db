package com.greatbee.core.bean.oi;

import com.greatbee.base.bean.AliasBean;
import com.greatbee.base.bean.IND;
import com.greatbee.base.bean.ext.SimpleIND;

/**
 * OI(Object Identified)
 * <p/>
 * Created by CarlChen on 16/10/11.
 */
public class OI extends SimpleIND implements IND, AliasBean {
    //别名
    private String alias;
    //资源地址，针对Mysql就是表名，针对Rest API就是content path，如果是LocalStorage，则是文件夹
    // /pic
    private String resource;
    //所属数据源别名
    private String dsAlias;

    public String getAlias() {
        return alias;
    }

    public String getResource() {
        return resource;
    }

    public String getDsAlias() {
        return dsAlias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public void setDsAlias(String dsAlias) {
        this.dsAlias = dsAlias;
    }
}
