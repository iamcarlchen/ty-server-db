package com.greatbee.core.manager.jsonExt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.base.util.Global;
import com.greatbee.base.util.JSONUtil;
import com.greatbee.base.util.StringUtil;
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
    public Connector getConnectorByAlias(String alias) {
        JSONObject dsObj = null;
        String resolveMode = Global.getInstance().getMode();
        if (StringUtil.isValid(resolveMode) && StringUtil.equals(resolveMode, "single_api_json")) {
            dsObj = Global.getInstance().findDs("connector_alias", alias);
        } else {
            dsObj = JSONUtil.getRootObjByAlias("connector", alias);
        }
        if (dsObj == null)
            return null;
        JSONArray ois = dsObj.getJSONArray(JSONSchema.JSON_Field_OIS);
        for (int i = 0; i < ois.size(); i++) {
            JSONObject oi = ois.getJSONObject(i);
            if (oi.containsKey(JSONSchema.JSON_Field_Connectors)) {
                JSONArray connectors = oi.getJSONArray(JSONSchema.JSON_Field_Connectors);
                for (int j = 0; j < connectors.size(); j++) {
                    JSONObject connector = connectors.getJSONObject(j);
                    if (connector.containsKey(JSONSchema.JSON_Field_Alias) && connector.getString(JSONSchema.JSON_Field_Alias).equals(alias)) {
                        connector = JSONUtil.camelJsonName(connector, null);
                        return (Connector)connector.toJavaObject(Connector.class);
                    }
                }
            }
        }
        return null;
    }

    public List<Connector> getConnectorByFromOiAlias(String fromOiAlias) throws DBException {
        List<Connector> cs = new ArrayList<>();
        JSONObject dsObj = null;
        String resolveMode = Global.getInstance().getMode();
        if (StringUtil.isValid(resolveMode) && StringUtil.equals(resolveMode, "single_api_json")) {
            dsObj = Global.getInstance().findDs("connector_foi", fromOiAlias);
        } else {
            dsObj = JSONUtil.getRootObjByAlias("oi", fromOiAlias);
        }
        if (dsObj == null)
            return null;
        JSONArray ois = dsObj.getJSONArray(JSONSchema.JSON_Field_OIS);
        for (int i = 0; i < ois.size(); i++) {
            JSONObject oi = ois.getJSONObject(i);
            if (oi.containsKey(JSONSchema.JSON_Field_Connector)) {
                JSONArray connectors = oi.getJSONArray(JSONSchema.JSON_Field_Connector);
                for (int j = 0; j < connectors.size(); j++) {
                    JSONObject connector = connectors.getJSONObject(j);
                    if (connector.containsKey(JSONSchema.JSON_Field_From_Oi_Alias) && connector.getString(JSONSchema.JSON_Field_From_Oi_Alias).equals(fromOiAlias))
                        cs.add(connector.toJavaObject(Connector.class));
                }
            }
        }
        return cs;
    }
}
