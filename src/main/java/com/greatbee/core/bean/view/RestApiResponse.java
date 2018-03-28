package com.greatbee.core.bean.view;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by usagizhang on 18/3/28.
 */
public class RestApiResponse {
    private String responseBody;
    private Map<String, String> cookie;
    private Map<String, String> header;

    public RestApiResponse(String responseBody) {
        this.responseBody = responseBody;
        cookie = new HashMap<String, String>();
        header = new HashMap<String, String>();
    }

    public RestApiResponse(String responseBody, Map<String, String> cookie, Map<String, String> header) {
        this.responseBody = responseBody;
        this.cookie = cookie;
        this.header = header;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public void setResponseBody(String responseBody) {
        this.responseBody = responseBody;
    }

    public Map<String, String> getCookie() {
        return cookie;
    }

    public void setCookie(Map<String, String> cookie) {
        this.cookie = cookie;
    }

    public Map<String, String> getHeader() {
        return header;
    }

    public void setHeader(Map<String, String> header) {
        this.header = header;
    }

    /**
     * 添加header的kv值
     *
     * @param key
     * @param value
     */
    public void addHeader(String key, String value) {
        this.header.put(key, value);
    }

    /**
     * 添加cookie的key值
     *
     * @param key
     * @param value
     */
    public void addCookie(String key, String value) {
        this.cookie.put(key, value);
    }

    /**
     * 获取json对象
     *
     * @return
     */
    public JSONObject toJSONObject() {
        return JSON.parseObject(this.getResponseBody());
    }
}
