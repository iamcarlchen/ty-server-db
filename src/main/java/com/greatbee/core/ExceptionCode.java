package com.greatbee.core;

/**
 * Author :CarlChen
 * Date:17/7/18
 */
public interface ExceptionCode {
    //空
    String NULL = "NULL";
    String TIP = "一键生成";
    //TY配置登录session
    String TY_SESSION_CONFIG_USER = "TY_SESSION_CONFIG_USER";
    //inputField 模板 request数据
    String TY_INPUTFIELD_TPL_REQUEST = "request";
    //inputField 模板 session数据
    String TY_INPUTFIELD_TPL_SESSION = "session";
    //inputField 模板 节点数据
    String TY_INPUTFIELD_TPL_NODE = "node";

    //没有登录session返回报错
    long CODE_NO_LOGIN_SESSION = -400;

    //流程中断代码
    public static final long CODE_Process_Interrupt = 1;
    //流程继续代码
    public static final long CODE_Process_Continue = 2;

    //常用错误异常Code以1开头
    public static final long ERROR_API_NOT_VALID = 100001;
    public static final long ERROR_API_NO_LEGO = 100002;
    public static final long ERROR_API_LEGO_CODE_NOT_FOUND = 100003;
    public static final long ERROR_API_METHOD_NOT_MATCH = 100004;
    public static final long ERROR_API_CONFIG_NO_TPL = 100005;
    public static final long ERROR_API_REQUEST_ENCODE = 100006;

    //DB的相关异常Code以2开头
    public static final long ERROR_DB_DS_NOT_FOUND = 200001;//数据源没有找到
    public static final long ERROR_DB_SQL_EXCEPTION = 200002;//执行sql时出错
    public static final long ERROR_DB_FIELD_LENGTH_OVER_LIMIT = 200003;//create或者update字段值长度超过限制
    public static final long ERROR_DB_OI_TABLE_NAME_INVAlID = 200004;//OI表名无效
    public static final long ERROR_DB_CONN_CLOSE_ERROR = 200005;//关闭connection错误
    public static final long ERROR_DB_PS_CLOSE_ERROR = 200006;//关闭preparestatment错误
    public static final long ERROR_DB_RS_CLOSE_ERROR = 200007;//关闭Resultset错误
    public static final long ERROR_DB_CONT_IS_NULL = 200008;//链接查询connector为空
    public static final long ERROR_DB_DST_NOT_SUPPORT = 200009;//数据源类型不支持
    public static final long ERROR_DB_OI_INVAlID = 200010;//OI为空
    public static final long ERROR_DB_OI_DS_INVAlID = 200011;//OI的DS为空
    public static final long ERROR_DB_OI_ALIAS_INVAlID = 200012;//OI的Alias为空
    public static final long ERROR_DB_OIView_INVAlID = 200013;//OIView为空
    public static final long ERROR_DB_FIELD_NOT_EXIST = 200014;//字段不存在为空
    public static final long ERROR_DB_FIELD_EXIST = 200015;//字段已存在
    public static final long ERROR_DB_TABLE_NOT_EXIST = 200016;//表不存在
    public static final long ERROR_DB_TABLE_EXIST = 200017;//表已存在

    //Lego的相关异常Code以3开头
    public static final long ERROR_LEGO_ADD = 300001;
    public static final long ERROR_LEGO_ADD_NO_FIELDS = 300002;
    public static final long ERROR_LEGO_READ_NO_FIELD = 300003;
    public static final long ERROR_LEGO_LIST_NO_OI = 300004;
    public static final long ERROR_LEGO_UPDATE = 300005;
    public static final long ERROR_LEGO_UPDATE_NO_FIELDS = 300006;
    public static final long ERROR_LEGO_IMPORT_OVER_MAXRECORD = 300007;//导入数量超过最大限制
    public static final long ERROR_LEGO_IMPORT_FILE_IO_ERROR = 300008;
    public static final long ERROR_LEGO_IMPORT_NOT_XLSX = 300009;
    public static final long ERROR_LEGO_STREAM_CLOSE_ERROR = 300010;
    public static final long ERROR_LEGO_FILE_EMPTY = 300010;
    public static final long ERROR_LEGO_FILE_SAVE_FAIL = 300010;
    public static final long ERROR_LEGO_FILE_Not_Found = 300010;
    public static final long ERROR_LEGO_DOWN_FILE_PARAM_INVAILD = 300013;
    public static final long ERROR_LEGO_EXPORT_FILE_IO_ERROR = 300014;
    public static final long ERROR_LEGO_SESSION_TIMEOUT = 300015;
    public static final long ERROR_LEGO_IMPORT_NO_FILE = 300015;
    public static final long ERROR_LEGO_VALUE_MD5_ERROR = 300015;
    public static final long ERROR_LEGO_VALUE_CREATE_VC_ERROR = 300015;
    public static final long ERROR_LEGO_VALUE_VC_ERROR = 300015;
    public static final long ERROR_LEGO_OI_ALIAS_PARAM_ERROR = 300015;
    public static final long ERROR_LEGO_INIT_DB_ERROR = 300015;
    public static final long ERROR_LEGO_DS_ALIAS_INVALIDATE = 300015;
    public static final long ERROR_LEGO_GENERATE = 300016;
    public static final long ERROR_LEGO_CUSTOM_CONDITION_PARAM = 300016;

    //字段验证相关异常以4开头
    public static final long ERROR_FIELD_VALIDATE_PARAMS_INVALID = 400001;
    public static final long ERROR_FIELD_VALIDATE_REQUIRED = 400002;
    public static final long ERROR_FIELD_VALIDATE_NOT_DATE_TYPE = 400002;
    public static final long ERROR_FIELD_VALIDATE_NOT_NUMBER = 400002;
    public static final long ERROR_FIELD_VALIDATE_NOT_INTEGER = 400002;
    public static final long ERROR_FIELD_VALIDATE_OVER_LENGTH = 400002;
    public static final long ERROR_FIELD_VALIDATE_OVER_VALUE = 400002;
    public static final long ERROR_FIELD_VALIDATE_MD5_ERROR = 400002;
    public static final long ERROR_FIELD_VALIDATE_VALUE_INVALID = 400002;
    public static final long ERROR_FIELD_VALIDATE_DATE_TRANSFER = 400002;

    //字段格式化相关异常以5开头
    public static final long ERROR_FIELD_HANDLE_VALUE_IS_NULL = 500001;
    public static final long ERROR_FIELD_HANDLE_VALUE_IS_NOT_NULL = 500001;
    public static final long ERROR_FIELD_HANDLE_VALUE_INVALID = 500001;

}
