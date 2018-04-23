package com.greatbee.core.db;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * 非结构化数据管理器
 * Author: CarlChen
 * Date: 2018/3/20
 */
public interface UnstructuredDataManager {
    /**
     * 链接
     *
     * @param oi
     * @param fields
     * @return
     */
    public Object connect(HttpServletRequest request,OI oi, List<Field> fields) throws DBException;
}
