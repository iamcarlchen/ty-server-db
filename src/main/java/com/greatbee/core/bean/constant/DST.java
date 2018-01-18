package com.greatbee.core.bean.constant;

/**
 * DST(Data Source Type)
 * <p/>
 * Created by CarlChen on 16/10/11.
 */
public enum DST {
    Static("static"),

    //关系型
    Mysql("mysql"),
    Oracle("oracle"),
    SqlServer("sqlserver"),

    //非结构化
    RestAPI("rest_api"),
    Dubbo("dubbo"),
    Redis("redis"),

    //本地文件存储
    LocalStorage("local_storage");

    private String type;

    DST(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    /**
     * GET DST
     *
     * @param type
     * @return
     */
    public static final DST getDST(String type) {
        DST[] arr = DST.values();
        for (DST dst : arr) {
            if (dst.getType().equalsIgnoreCase(type)) {
                return dst;
            }
        }
        return null;
    }
}
