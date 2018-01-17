package com.greatbee.core.bean.view;

/**
 * 差异细节
 * <p/>
 * Author: CarlChen
 * Date: 2018/1/17
 */
public class DiffItem {
    /**
     * 差异类型
     * type=1: 物理表存在，TY没有配置
     * type=2: TY配置表存在，物理表没有
     * type=3: 物理表中的字段X存在，TY没有配置
     * type=4: TY表配置了字段X，物理表没有
     * type=5: TY表配置的字段和物理表配置的字段类型等参数不一致
     */
    private String type;

    //差异的相关表
    private String resource;
    //差异的相关字段
    private String fieldName;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
