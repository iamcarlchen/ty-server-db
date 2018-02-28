package com.greatbee.core.db;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.DiffItem;

import java.util.List;

/**
 * Schema Data Manager
 *
 * Author: CarlChen
 * Date: 2018/1/17
 */
public interface SchemaDataManager {
    /**
     * TY配置DS与物理的DS进行比较，把差异列表返回
     *
     * @param ds
     * @return
     * @throws DBException
     */
    public List<DiffItem> diff(DSView dsView) throws DBException;

    /**
     * 创建表
     *
     * @param oi
     * @throws DBException
     */
    public void createTable(OI oi,List<Field> dFields) throws DBException;

    /**
     * 删除表
     *
     * @param oi
     * @throws DBException
     */
    public void dropTable(OI oi) throws DBException;

    /**
     * 添加字段
     *
     * @param oi
     * @param field
     * @throws DBException
     */
    public void addField(OI oi, Field field) throws DBException;

    /**
     * 删除字段
     *
     * @param oi
     * @param field
     * @throws DBException
     */
    public void dropField(OI oi, Field field) throws DBException;

    /**
     * 更新字段
     *
     * @param oi
     * @param field
     * @throws DBException
     */
    public void updateField(OI oi, Field oldField, Field newField) throws DBException;
}
