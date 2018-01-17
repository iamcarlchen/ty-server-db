package com.greatbee.core.bean.oi;

import com.greatbee.base.bean.AliasBean;
import com.greatbee.base.bean.IND;
import com.greatbee.base.bean.ext.SimpleIND;

/**
 * 连接器
 * <p/>
 * Created by CarlChen on 16/10/11.
 */
public class Connector extends SimpleIND implements IND, AliasBean {
    //别名
    private String alias;
    //来自的OI
    private String fromOIAlias;
    //来自的Field
    private String fromFieldName;

    //目标OI
    private String toOIAlias;
    //目标字段
    private String toFieldName;

    public String getFromOIAlias() {
        return fromOIAlias;
    }

    public String getFromFieldName() {
        return fromFieldName;
    }

    public String getToOIAlias() {
        return toOIAlias;
    }

    public String getToFieldName() {
        return toFieldName;
    }

    public void setFromOIAlias(String fromOIAlias) {
        this.fromOIAlias = fromOIAlias;
    }

    public void setFromFieldName(String fromFieldName) {
        this.fromFieldName = fromFieldName;
    }

    public void setToOIAlias(String toOIAlias) {
        this.toOIAlias = toOIAlias;
    }

    public void setToFieldName(String toFieldName) {
        this.toFieldName = toFieldName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
