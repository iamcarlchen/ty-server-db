package com.greatbee.core.lego;

import java.util.List;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.server.InputField;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.DiffItem;
import com.greatbee.core.db.SchemaDataManager;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("schemaDiff")
public class SchemaDiff implements Lego, ExceptionCode {

    private static final Logger logger = Logger.getLogger(SchemaDiff.class);

    @Autowired
    private SchemaDataManager mysqlDataManager;

    /**
     * 执行
     */
    @Override
    public void execute(Input input, Output output) throws LegoException {

        //获取输入的DS
        InputField dsInputField = input.getInputField("ds");
        //校验输入的内容
        if (dsInputField == null || dsInputField.getFieldValue() == null) {
            //没有输入ds的alias
            throw new LegoException("缺少字段 input DSView", ERROR_FIELD_VALIDATE_PARAMS_INVALID);
        }
        //获取属性的内容
        DSView diffDSView = (DSView) dsInputField.getFieldValue();
        try {
            //调用接口
            List<DiffItem> diffItemList = mysqlDataManager.diff(diffDSView);
            //设置返回值
            output.setOutputValue("diffItemList", diffItemList);
        } catch (DBException e) {
            e.printStackTrace();
            logger.error("调用diff接口异常");
            logger.error(e);
        }
    }

}
