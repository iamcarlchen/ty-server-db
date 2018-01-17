package com.greatbee.core.db;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.DSView;

/**
 * 关系型数据管理
 * <p/>
 * Created by CarlChen on 2017/5/24.
 */
public interface RelationalDataManager extends DataManager {
    /**
     * 从物理数据库中导出OIView
     *
     * @param ds
     */
    public DSView exportFromPhysicsDS(DS ds) throws DBException;

    /**
     * 从DSView生成MysqlSchema
     * <p/>
     * 生成Create Table的Schema并且到DS执行
     *
     * @param dsView
     */
    public void importFromDS(DSView dsView) throws DBException;

    /**
     * 跨表读
     *
     * @param connectorTree
     * @return
     * @throws DBException
     */
    public Data read(ConnectorTree connectorTree) throws DBException;

    /**
     * 跨表翻页
     *
     * @param page
     * @param pageSize
     * @param connectorTree
     * @return
     * @throws DBException
     */
    public DataPage page(int page, int pageSize, ConnectorTree connectorTree) throws DBException;

    /**
     * 跨表列表
     *
     * @param connectorTree
     * @return
     * @throws DBException
     */
    public DataList list(ConnectorTree connectorTree) throws DBException;

    /**
     * 通过Connector Tree的Count
     *
     * @param connectorTree
     * @return
     * @throws DBException
     */
    public int count(ConnectorTree connectorTree) throws DBException;
}
