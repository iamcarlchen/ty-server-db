package com.greatbee.core.bean.oi;

import com.greatbee.base.bean.IND;
import com.greatbee.base.bean.ext.SimpleIND;

import java.util.List;

/**
 * Field
 * <p/>
 * Created by CarlChen on 16/10/11.
 */
public class Field extends SimpleIND implements IND {
    //字段名称
    private String fieldName;
    //所属OI的别名
    private String oiAlias;
    //数据类型
    private String dt;

    //所属父级字段
    private Integer parentFieldId;
    //字段列表，如果dt = object或者array，需要有更多子字段的描述，通过parentFieldId来获取
    private List<Field> fields;
    //是否是主键
    private boolean pk;
    //字段的值
    private String fieldValue;
    //字段长度
    private Integer fieldLength;

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }

    public boolean isPk() {
        return pk;
    }

    public void setPk(boolean pk) {
        this.pk = pk;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getOiAlias() {
        return oiAlias;
    }

    public String getDt() {
        return dt;
    }

    public Integer getParentFieldId() {
        return parentFieldId;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setOiAlias(String oiAlias) {
        this.oiAlias = oiAlias;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public void setParentFieldId(Integer parentFieldId) {
        this.parentFieldId = parentFieldId;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public Integer getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(Integer fieldLength) {
        this.fieldLength = fieldLength;
    }
}
