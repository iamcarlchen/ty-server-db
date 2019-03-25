package com.greatbee.core.manager.jsonExt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.JSONUtil;
import com.greatbee.core.bean.constant.JSONSchema;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.manager.ConnectorManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Author :Xiaobc
 * Date:19/3/25
 */
public class SimpleConnectorManager  extends AbstractBasicManager implements ConnectorManager {
    public SimpleConnectorManager() {
        super(Connector.class);
    }

    /**
     * {
     * 		ds_fields:'',
     * 		ois:[{
     * 			oi_fields:'',
     * 			fields:[{
     * 				field_fields:''
     *          }],
     * 			connector:[{	//fromOiAlias==oiAlias
     * 				connector_fields:''
     *           }]
     *       }]
     *    }
     * @param alias
     * @return
     * @throws DBException
     */
    @Override
    public Connector getConnectorByAlias(String alias) throws DBException {
        JSONObject dsObj = JSONUtil.readJsonFile(JSONUtil.Model_Path, JSONSchema.Mokelay_DS_Alias);
        if(dsObj==null){
            return null;
        }
        JSONArray ois = dsObj.getJSONArray(JSONSchema.JSON_Field_OIS);
        for(int i=0;i<ois.size();i++){
            JSONObject oi = ois.getJSONObject(i);
            if(!oi.containsKey(JSONSchema.JSON_Field_Connector)){
                continue;
            }
            JSONArray connectors = oi.getJSONArray(JSONSchema.JSON_Field_Connector);
            for(int j=0;j<connectors.size();j++){
                JSONObject connector = connectors.getJSONObject(j);
                if(connector.containsKey(JSONSchema.JSON_Field_Alias) && connector.getString(JSONSchema.JSON_Field_Alias).equals(alias)){
                    //找到了对应alias的连接器
                    return connector.toJavaObject(Connector.class);
                }
            }
        }
        return null;
    }

    @Override
    public List<Connector> getConnectorByFromOiAlias(String fromOiAlias) throws DBException {
        List<Connector> cs = new ArrayList<>();
        JSONObject dsObj = JSONUtil.readJsonFile(JSONUtil.Model_Path, JSONSchema.Mokelay_DS_Alias);
        if(dsObj==null){
            return null;
        }
        JSONArray ois = dsObj.getJSONArray(JSONSchema.JSON_Field_OIS);
        for(int i=0;i<ois.size();i++){
            JSONObject oi = ois.getJSONObject(i);
            if(!oi.containsKey(JSONSchema.JSON_Field_Connector)){
                continue;
            }
            JSONArray connectors = oi.getJSONArray(JSONSchema.JSON_Field_Connector);
            for(int j=0;j<connectors.size();j++){
                JSONObject connector = connectors.getJSONObject(j);
                if(connector.containsKey(JSONSchema.JSON_Field_From_Oi_Alias) && connector.getString(JSONSchema.JSON_Field_From_Oi_Alias).equals(fromOiAlias)){
                    //找到了对应alias的连接器
                    cs.add(connector.toJavaObject(Connector.class));
                }
            }
        }
        return cs;
    }
}
