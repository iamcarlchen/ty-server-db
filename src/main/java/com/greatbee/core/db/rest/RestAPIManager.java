package com.greatbee.core.db.rest;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.RestApiFieldGroupType;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.RestApiResponse;
import com.greatbee.core.db.UnstructuredDataManager;
import com.greatbee.core.lego.LegoException;
import com.greatbee.core.lego.util.TemplateUtil;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.util.HttpClientUtil;
import com.greatbee.core.util.OIUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rest API Manager
 * <p/>
 * Author: CarlChen
 * Date: 2018/3/20
 */
public class RestAPIManager implements UnstructuredDataManager, ExceptionCode {

    private static final String METHOD_TYPE_POST = "post";
    private static final String METHOD_TYPE_GET = "get";

    @Autowired
    private DSManager dsManager;

    @Override
    public Object connect(HttpServletRequest request,OI oi, List<Field> fields) throws DBException {
        //验证oi是否有效
        OIUtils.isValid(oi);
        DS ds = dsManager.getDSByAlias(oi.getDsAlias());
        if (ds == null) {
            throw new DBException("数据源没有找到", ERROR_DB_DS_NOT_FOUND);
        } else if (StringUtil.isInvalid(ds.getConnectionUrl())) {
            throw new DBException("rest api服务器数据源没有配置", ERROR_DB_DS_NOT_FOUND);
        }

        String method = _buildingMethod(fields);
        StringBuilder requestURLBuilder = new StringBuilder(_buildApiRestUrl(ds.getConnectionUrl(), request));
        requestURLBuilder.append(oi.getResource());
        RestApiResponse data = null;
        if (method.equalsIgnoreCase(METHOD_TYPE_GET)) {
            //post请求
            data = HttpClientUtil.get(_setReuqetPathParm(requestURLBuilder, fields), _buildingHeaderBody(fields));
        } else if (method.equalsIgnoreCase(METHOD_TYPE_POST)) {
            //get请求
            data = HttpClientUtil.post(_setReuqetPathParm(requestURLBuilder, fields), _buildingPostBody(fields), _buildingHeaderBody(fields));
        }

        return data;
    }

    /**
     * 模板解析url（主要针对不同环境请求地址解析）
     * @param tpl
     * @param request
     * @return
     */
    private String _buildApiRestUrl(String tpl,HttpServletRequest request) {
        if(request==null){
            return tpl;
        }
        String result = tpl;
        Map<String, Object> params = new HashMap<>();
        try {
            params.put("request", TemplateUtil.getParameterMap(request));
            params.put("session", TemplateUtil.getSessionAttributeMap(request));
            String serverName = request.getServerName();
            String[] hostArray = serverName.split("\\.");
            params.put("serverName", serverName);
            params.put("hostArray", hostArray);
            result = TemplateUtil.transferInputValue(tpl, params);
        }catch(LegoException e){
            return result;
        }
        return result;
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
                    urlBuilder = new StringBuilder(urlBuilder.toString().replaceAll("\\{" + field.getFieldName() + "\\}", StringUtil.getString(field.getFieldValue(), "")));
                }
            }
        }
        //构建url上的queryString参数
        if (CollectionUtil.isValid(fields)) {
            StringBuilder queryStringBuilder = new StringBuilder("?");
            for (Field field : fields) {
                if (StringUtil.isValid(field.getGroup()) && field.getGroup().equalsIgnoreCase(RestApiFieldGroupType.Get.getType())) {
                    try {
                        queryStringBuilder.append(field.getFieldName()).append("=").append(URLEncoder.encode(field.getFieldValue(), "UTF8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
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
