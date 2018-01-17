package com.greatbee.core.bean.constant;

/**
 * DSCF(Data Source Config From)
 * <p/>
 * Created by CarlChen on 16/10/11.
 */
public enum DSCF {
    //从数据库获取
    SQL("sql"),
    //配置文件
    FILE("file");

    private String type;

    DSCF(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
