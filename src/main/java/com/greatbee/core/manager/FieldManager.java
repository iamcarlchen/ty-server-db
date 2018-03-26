package com.greatbee.core.manager;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.BasicManager;
import com.greatbee.core.bean.oi.Field;

import java.util.List;

/**
 * Created by CarlChen on 16/10/11.
 */
public interface FieldManager extends BasicManager {
    /**
     * Field List
     *
     * @param oiAlias
     * @return
     * @throws DBException
     */
    public List<Field> getFields(String oiAlias) throws DBException;

    /**
     * 获取字段
     * @param oiAlias
     * @param fieldName
     * @return
     * @throws DBException
     */
    public List<Field> getFields(String oiAlias,String fieldName) throws DBException;

}
