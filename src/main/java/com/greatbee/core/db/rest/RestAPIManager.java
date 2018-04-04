package com.greatbee.core.db.rest;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.bean.constant.RestApiFieldGroupType;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.UnstructuredDataManager;
import com.greatbee.core.util.HttpClientUtil;
import com.greatbee.core.util.OIUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rest API Manager
 * <p/>
 * Author: CarlChen
 * Date: 2018/3/20
 */
public class RestAPIManager implements UnstructuredDataManager {

    private static final String METHOD_TYPE_POST = "post";
    private static final String METHOD_TYPE_GET = "get";


    @Override
    public String connect(OI oi, List<Field> fields) throws DBException {
        //验证oi是否有效
        OIUtils.isValid(oi);
        String method = _buildingMethod(fields);
        StringBuilder requestURLBuilder = new StringBuilder();
        requestURLBuilder.append(oi.getResource());
        JSONObject data = null;
        if (method.equalsIgnoreCase(METHOD_TYPE_POST)) {
            //post请求
            data = HttpClientUtil.httpGet(_setReuqetPathParm(requestURLBuilder, fields), _buildingHeaderBody(fields));
        } else if (method.equalsIgnoreCase(METHOD_TYPE_GET)) {
            //get请求
            data = HttpClientUtil.post(_setReuqetPathParm(requestURLBuilder, fields), _buildingPostBody(fields), _buildingHeaderBody(fields), false);
        }

        return data.toJSONString();
    }


    /**
     * 设置URL上的参数
     *
     * @param urlBuilder
     * @param fields
     * @return
     */
    private String _setReuqetPathParm(StringBuilder urlBuilder, List<Field> fields) {
        //构建url上的path参数
        if (CollectionUtil.isValid(fields)) {
            for (Field field : fields) {
                if (StringUtil.isValid(field.getGroup()) && field.getGroup().equalsIgnoreCase(RestApiFieldGroupType.Path.getType())) {
                    urlBuilder = new StringBuilder(urlBuilder.toString().replaceAll("{" + field.getFieldName() + "}", StringUtil.getString(field.getFieldValue(), "")));
                }
            }
        }
        //构建url上的queryString参数
        if (CollectionUtil.isValid(fields)) {
            StringBuilder queryStringBuilder = new StringBuilder("?");
            for (Field field : fields) {
                if (StringUtil.isValid(field.getGroup()) && field.getGroup().equalsIgnoreCase(RestApiFieldGroupType.Get.getType())) {
                    queryStringBuilder.append(field.getFieldName()).append("=").append(field.getFieldValue());
                }
            }
            if (queryStringBuilder.length() > 1) {
                urlBuilder.append(queryStringBuilder);
            }
        }
        return urlBuilder.toString();
    }

    /**
     * 设置post参数
     *
     * @param fields
     * @return
     */
    private Map<String, String> _buildingPostBody(List<Field> fields) {
        Map<String, String> requestBody = new HashMap<String, String>();
        if (CollectionUtil.isValid(fields)) {
            for (Field field : fields) {
                if (StringUtil.isValid(field.getGroup()) && field.getGroup().equalsIgnoreCase(RestApiFieldGroupType.Post.getType())) {
                    requestBody.put(field.getFieldName(), field.getFieldValue());
                }
            }
        }
        return requestBody;
    }

    /**
     * 设置header参数
     *
     * @param fields
     * @return
     */
    private Map<String, String> _buildingHeaderBody(List<Field> fields) {
        Map<String, String> requestBody = new HashMap<String, String>();
        if (CollectionUtil.isValid(fields)) {
            for (Field field : fields) {
                if (StringUtil.isValid(field.getGroup()) && field.getGroup().equalsIgnoreCase(RestApiFieldGroupType.Header.getType())) {
                    requestBody.put(field.getFieldName(), field.getFieldValue());
                }
            }
        }
        return requestBody;
    }

    /**
     * 设置method
     *
     * @param fields
     * @return
     */
    private String _buildingMethod(List<Field> fields) {
        String method = METHOD_TYPE_GET;
        if (CollectionUtil.isValid(fields)) {
            for (Field field : fields) {
                if (StringUtil.isValid(field.getGroup()) && field.getGroup().equalsIgnoreCase(RestApiFieldGroupType.Method.getType())) {
                    method = field.getFieldValue();
                }
            }
        }
        return method;
    }
}
