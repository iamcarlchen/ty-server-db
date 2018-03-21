package com.greatbee.core.db.rest;

import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.UnstructuredDataManager;

import java.util.List;

/**
 * Rest API Manager
 * <p/>
 * Author: CarlChen
 * Date: 2018/3/20
 */
public class RestAPIManager implements UnstructuredDataManager {

    //字段Groups
    private static final String[] Field_Groups = new String[]{"Header", "Post", "Get", "Path"};

    @Override
    public String connect(OI oi, List<Field> fields) {
        //TODO
        return null;
    }
}
