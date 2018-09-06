package com.greatbee.core.util;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.ArrayUtil;
import com.greatbee.base.util.Charset;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.view.RestApiResponse;
import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

//import com.sun.deploy.net.HttpResponse;
//import org.springframework.http.HttpEntity;

/**
 * NVWA HTTP
 * <p>
 * Created by lufaxdev on 2014/11/25.
 */
public class HttpClientUtil implements ExceptionCode {
    public static String KEY_HEADER = "header";
    public static String KEY_BODY = "body";

    /**
     * Invoke
     *
     * @param httpclient
     * @param request
     * @return
     */
    private static RestApiResponse invoke(HttpClient httpclient,
                                          HttpUriRequest request) {
        HttpResponse response = null;

        try {
            response = httpclient.execute(request);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        HttpEntity entity = response.getEntity();
        String body = null;
        Header[] headers = response.getAllHeaders();

        try {
            body = EntityUtils.toString(entity, "UTF-8");
            //设置response body
            RestApiResponse restApiResponse = new RestApiResponse(body);
            if (ArrayUtil.isValid(headers)) {
                for (Header item : headers) {
                    restApiResponse.addHeader(item.getName(), item.getValue());
                }
            }
            return restApiResponse;
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Get
     *
     * @param url
     * @return
     */
    public static RestApiResponse get(String url, Map<String, String> headerParams) throws DBException {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(url);
        if (CollectionUtil.isValid(headerParams)) {
            //设置header参数
            Set<String> headerKeySet = headerParams.keySet();
            for (String key : headerKeySet) {
                httpget.setHeader(key, headerParams.get(key));
            }
        }
        RestApiResponse response = invoke(httpClient, httpget);
        httpClient.getConnectionManager().shutdown();
        if (StringUtil.isInvalid(response.getResponseBody())) {
            throw new DBException("response is null", ERROR_DB_HTTP_ERROR);
        }
        return response;
    }

    /**
     * POST
     *
     * @param url
     * @param params
     * @return
     */
    public static RestApiResponse post(String url, Map<String, String> params) {
        return post(url, params, null);
    }

    public static void main(String[] args) {
        Map map = new HashMap<>();
        map.put("name","测试");
        map.put("age", "20");

        Map head = new HashMap<>();
        head.put("x-xiaoyi-date","2018-09-06 19:16:00");
        post("http://local.dev.rs.com:8080/config/test",map,head);
    }


    /**
     * multipart/form-data 方式提交
     *
     * @param url
     * @param params
     * @param headerParams
     * @return
     */
    public static RestApiResponse multipartPost(String url, Map<String, String> params, Map<String, String> headerParams) {
        System.out.println(url);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        // httpClint的请求等待超时时间设定5秒
        HttpPost httpost = new HttpPost(url);

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        Set<String> keySet = params.keySet();
        ContentType contentType = ContentType.create(HTTP.PLAIN_TEXT_TYPE, HTTP.UTF_8);
        for (String key : keySet) {
            builder.addTextBody(key, params.get(key), contentType);
        }
        HttpEntity reqEntity = builder.build();
        httpost.setEntity(reqEntity);

        if (CollectionUtil.isValid(headerParams)) {
            //设置header参数
            Set<String> headerKeySet = headerParams.keySet();
            for (String key : headerKeySet) {
                httpost.setHeader(key, headerParams.get(key));
            }
        }

        RestApiResponse restApiResponse = null;
        try {
            CloseableHttpResponse response = httpClient.execute(httpost);
            HttpEntity responseEntity = response.getEntity();
            String sResponse=EntityUtils.toString(responseEntity, "UTF-8");
            System.out.println("Post 返回结果" + sResponse);
            restApiResponse = new RestApiResponse(sResponse);
            Header[] headers = response.getAllHeaders();
            if (ArrayUtil.isValid(headers)) {
                for (Header item : headers) {
                    restApiResponse.addHeader(item.getName(), item.getValue());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return restApiResponse;
    }


    /**
     * POST
     *
     * @param url
     * @param params
     * @return
     */
    public static RestApiResponse post(String url, Map<String, String> params, Map<String, String> headerParams) {
        System.out.println(url);
        DefaultHttpClient httpClient = new DefaultHttpClient();
        // httpClint的请求等待超时时间设定5秒
        httpClient.getParams().setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000);
        httpClient.getParams().setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000);
        HttpPost httpost = new HttpPost(url);
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();


        StringBuffer sb = new StringBuffer();

        Set<String> keySet = params.keySet();
        for (String key : keySet) {
            nvps.add(new BasicNameValuePair(key, params.get(key)));
            sb.append(key + "=" + params.get(key) + "&");
        }
        String getUrl = url + "?" + sb.toString();
        System.out.println(DateUtils.formatDate(new Date(), "yyyy-MM-dd hh:mm:ss") + getUrl);
        try {
            httpost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
            if (CollectionUtil.isValid(headerParams)) {
                //设置header参数
                Set<String> headerKeySet = headerParams.keySet();
                for (String key : headerKeySet) {
                    httpost.setHeader(key, headerParams.get(key));
                }
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        RestApiResponse response = invoke(httpClient, httpost);
        return response;
    }

    //TODO:暂时不用的函数

    public static String sendPostBody(String postUrl, String postBody) throws DBException {
        try {
            if (StringUtil.isInvalid(postUrl)) {
                throw new DBException("url is null", ERROR_DB_HTTP_ERROR);
            } else if (StringUtil.isInvalid(postBody)) {
                throw new DBException("post body is null", ERROR_DB_HTTP_ERROR);
            }
            // Configure and open a connection to the site you will send the request
            URL url = new URL(postUrl);
            URLConnection urlConnection = url.openConnection();
            // 设置doOutput属性为true表示将使用此urlConnection写入数据
            urlConnection.setDoOutput(true);
            // 定义待写入数据的内容类型，我们设置为application/x-www-form-urlencoded类型
            urlConnection.setRequestProperty("content-type", "application/x-www-form-urlencoded");
            // 得到请求的输出流对象
            OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream(), Charset.UTF_8);

            // 把数据写入请求的Body
            out.write(postBody);
            out.flush();
            out.close();

            // 从服务器读取响应
            InputStream inputStream = urlConnection.getInputStream();
            String encoding = urlConnection.getContentEncoding();
            String response = IOUtils.toString(inputStream, encoding);
            return response;
        } catch (IOException e) {
            throw new DBException(e.getMessage(), ERROR_DB_HTTP_ERROR);
        }
    }

    public static String sendPostBody(String postUrl, String postBody, int timeOut) throws DBException {
        try {
            if (StringUtil.isInvalid(postUrl)) {
                throw new DBException("url is null", ERROR_DB_HTTP_ERROR);
            } else if (StringUtil.isInvalid(postBody)) {
                throw new DBException("post body is null", ERROR_DB_HTTP_ERROR);
            }
            // Configure and open a connection to the site you will send the request
            URL url = new URL(postUrl);
            URLConnection urlConnection = url.openConnection();
            // 设置doOutput属性为true表示将使用此urlConnection写入数据
            urlConnection.setDoOutput(true);
            // 定义待写入数据的内容类型，我们设置为application/x-www-form-urlencoded类型
            urlConnection.setRequestProperty("content-type", "application/x-www-form-urlencoded");
            urlConnection.setConnectTimeout(timeOut);
            urlConnection.setReadTimeout(timeOut);
            // 得到请求的输出流对象
            OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream(), Charset.UTF_8);

            // 把数据写入请求的Body
            out.write(postBody);
            out.flush();
            out.close();

            // 从服务器读取响应
            InputStream inputStream = urlConnection.getInputStream();
            String encoding = urlConnection.getContentEncoding();
            String response = IOUtils.toString(inputStream, encoding);
            return response;
        } catch (IOException e) {
            throw new DBException(e.getMessage(), ERROR_DB_HTTP_ERROR);
        }
    }

    public static File saveUrlAs(String url, String filePath, String method) {
        //System.out.println("fileName---->"+filePath);
        //创建不同的文件夹目录
        File file = new File(filePath);
        //判断文件夹是否存在
//        if (!file.exists()) {
//            //如果文件夹不存在，则创建新的的文件夹
//            file.mkdirs();
//        }
        FileOutputStream fileOut = null;
        HttpURLConnection conn = null;
        InputStream inputStream = null;
        try {
            // 建立链接
            URL httpUrl = new URL(url);
            conn = (HttpURLConnection) httpUrl.openConnection();
            //以Post方式提交表单，默认get方式
            conn.setRequestMethod(method);
            conn.setDoInput(true);
            conn.setDoOutput(true);
            // post方式不能使用缓存
            conn.setUseCaches(false);
            //连接指定的资源
            conn.connect();
            //获取网络输入流
            inputStream = conn.getInputStream();
            BufferedInputStream bis = new BufferedInputStream(inputStream);
            //判断文件的保存路径后面是否以/结尾
//            if (!filePath.endsWith("/")) {
//
//                filePath += "/";
//
//            }
            //写入到文件（注意文件保存路径的后面一定要加上文件的名称）
            fileOut = new FileOutputStream(filePath);
            BufferedOutputStream bos = new BufferedOutputStream(fileOut);

            byte[] buf = new byte[4096];
            int length = bis.read(buf);
            //保存文件
            while (length != -1) {
                bos.write(buf, 0, length);
                length = bis.read(buf);
            }
            bos.close();
            bis.close();
            conn.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("抛出异常！！");
        }

        return file;

    }
}

