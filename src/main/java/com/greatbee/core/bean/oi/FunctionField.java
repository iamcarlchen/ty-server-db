package com.greatbee.core.bean.oi;

/**
 * FunctionField
 *
 * @author xiaobc
 * @date 17/11/20
 */
public class FunctionField extends Field {

    private String fn;
    private String params;

    public String getFn() {
        return fn;
    }

    public void setFn(String fn) {
        this.fn = fn;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }
}
