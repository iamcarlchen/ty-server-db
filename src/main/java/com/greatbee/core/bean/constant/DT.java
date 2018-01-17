package com.greatbee.core.bean.constant;

/**
 * DT(Data type数据类型)
 * <p/>
 * Created by CarlChen on 16/10/11.
 */
public enum DT {
    INT("int"),
    Double("double"),
    Boolean("boolean"),
    String("string"),
    Date("date"),
    Time("time"),
    Object("object"),
    Array("array"),
    Multipart("multipart");

    private String type;

    DT(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static DT getDT(String type) {
        DT[] l = DT.values();
        for (DT v : l) {
            if (v.getType().equalsIgnoreCase(type)) {
                return v;
            }
        }
        return String;
    }
}
