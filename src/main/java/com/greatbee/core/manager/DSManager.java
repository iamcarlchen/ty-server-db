package com.greatbee.core.manager;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.BasicManager;
import com.greatbee.core.bean.oi.DS;

/**
 * DS Manager
 * <p/>
 * Created by CarlChen on 16/10/11.
 */
public interface DSManager extends BasicManager {

    /**
     * 根据ds的alias 查询DS详情
     *
     * @param alias
     * @return
     */
    public DS getDSByAlias(String alias) throws DBException;
}
