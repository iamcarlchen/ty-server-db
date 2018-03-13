package com.greatbee.core.db;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.transaction.BaseTransactionTemplate;
import com.greatbee.core.bean.view.Condition;

import java.util.List;

/**
 * Data Manager
 * <p/>
 * Created by CarlChen on 2016/11/11.
 */
public interface DataManager {
    /**
     * 根据PK字段来读取一个唯一对象
     *
     * @param oi
     * @param fields
     * @param pkField
     * @return
     */
    public Data read(OI oi, List<Field> fields, Field pkField) throws DBException;

    /**
     * 翻页读取数据
     *
     * @param oi
     * @param fields
     * @param page
     * @param pageSize
     * @param condition
     * @return
     */
    public DataPage page(OI oi, List<Field> fields, int page, int pageSize, Condition condition) throws DBException;

    /**
     * 读取列表数据
     *
     * @param oi
     * @param fields
     * @param condition
     * @return
     */
    public DataList list(OI oi, List<Field> fields, Condition condition) throws DBException;

    /**
     * 根据PK字段删除一个唯一对象数据
     *
     * @param oi
     * @param pkField
     */
    public void delete(OI oi, Field pkField) throws DBException;

    /**
     * 根据条件来批量删除数据
     *
     * @param oi
     * @param condition
     */
    public void delete(OI oi, Condition condition) throws DBException;

    /**
     * 创建数据
     *
     * @param oi
     * @param fields
     * @return 返回创建数据的唯一ID
     */
    public String create(OI oi, List<Field> fields) throws DBException;

    /**
     * 根据PK字段更新一个唯一对象数据
     *
     * @param oi
     * @param fields
     * @param pkField
     */
    public void update(OI oi, List<Field> fields, Field pkField) throws DBException;

    /**
     * 根据条件批量更新数据
     *
     * @param oi
     * @param fields
     * @param condition
     */
    public void update(OI oi, List<Field> fields, Condition condition) throws DBException;

    /**
     * 执行事务(http://iweb.corp.rs.com/issues/61026)
     *
     * @throws DBException
     */
    public void executeTransaction(DS ds, List<BaseTransactionTemplate> transactionNodes) throws DBException;
}
