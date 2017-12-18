package com.greatbee.core.manager.data.bean.constant;

/**
 * Created by usagizhang on 17/12/15.
 */
public enum DSCF {
    SQL("sql"),
    FILE("file");

    private String type;

    private DSCF(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }
}
