package com.greatbee.core.manager.data.bean.constant;

/**
 * Created by usagizhang on 17/12/15.
 */
public enum DST {

    Static("static"),
    Mysql("mysql"),
    Oracle("oracle"),
    SqlServer("sqlserver"),
    RestAPI("rest_api"),
    Dubbo("dubbo"),
    Redis("redis"),
    LocalStorage("local_storage");

    private String type;

    private DST(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }
}
