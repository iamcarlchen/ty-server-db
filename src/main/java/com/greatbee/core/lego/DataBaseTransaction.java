package com.greatbee.core.lego;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.server.InputField;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.db.RelationalDataManager;
import com.greatbee.core.db.base.BaseTransactionTemplate;
import com.greatbee.core.db.mysql.transaction.MysqlCreateTransaction;
import com.greatbee.core.db.mysql.transaction.MysqlDeleteTransaction;
import com.greatbee.core.db.mysql.transaction.MysqlUpdateTransaction;
import com.greatbee.core.manager.DSManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 18/3/13.
 */
@Component("dataBaseTransaction")
public class DataBaseTransaction implements Lego, ExceptionCode {

    private static final Logger logger = Logger.getLogger(DataBaseTransaction.class);

    @Autowired
    private RelationalDataManager mysqlDataManager;
    @Autowired
    private DSManager dsManager;

    public static void main(String[] arg) {
        String a = "123123123";
        OI oi = new OI();
        Object objA = a;
        Object objOI = oi;
        System.out.println(a.getClass().getName());
        System.out.println(oi.getClass().getName());
        System.out.println(objA.getClass().getName());
        System.out.println(objOI.getClass().getName());

    }

    @Override
    public void execute(Input input, Output output) throws LegoException {

        InputField dsAlias = input.getInputField("ds");
        InputField transactions = input.getInputField("dataList");

        boolean dataListIsJson = transactions.getFieldValue().getClass().getName().equalsIgnoreCase("String");

        try {
            //组装transaction队列
            List<BaseTransactionTemplate> list = new ArrayList<>();
            if (dataListIsJson) {
                JSONArray transactionJSON = JSON.parseArray(StringUtil.getString(transactions.getFieldValue()));
                if (transactionJSON.size() > 0) {
                    for (int i = 0; i < transactionJSON.size(); i++) {
                        JSONObject transactionNodeJSON = transactionJSON.getJSONObject(i);
                        if (!transactionNodeJSON.containsKey("type")) {
                            throw new LegoException("type缺失", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
                        }
                        if (!transactionNodeJSON.containsKey("oi")) {
                            throw new LegoException("oi缺失", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
                        }

                        if (!transactionNodeJSON.containsKey("conditions")) {
                            //没有condition字段
                        }
                        String type = transactionNodeJSON.getString("type");
                        JSONObject oiJSON = transactionNodeJSON.getJSONObject("oi");
                        OI oi = oiJSON.toJavaObject(OI.class);
                        if (oi == null || StringUtil.isInvalid(oi.getAlias()) || StringUtil.isInvalid(oi.getResource())) {
                            throw new LegoException("oi字段缺失", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
                        }
                        if (StringUtil.isValid(type) && type.equalsIgnoreCase("create")) {
                            //create
                            if (!transactionNodeJSON.containsKey("fields")) {
                                //没有field字段
                                throw new LegoException("fields缺失", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
                            }
                            JSONArray fieldJSON = transactionNodeJSON.getJSONArray("fields");
                            List<Field> fieldList = JSON.parseArray(fieldJSON.toJSONString(), Field.class);
                            BaseTransactionTemplate createNode = new MysqlCreateTransaction(oi, fieldList);
                            list.add(createNode);
                        } else if (StringUtil.isValid(type) && type.equalsIgnoreCase("update")) {
                            //update
                            if (!transactionNodeJSON.containsKey("fields")) {
                                //没有field字段
                                throw new LegoException("fields缺失", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
                            }
                            JSONArray fieldJSON = transactionNodeJSON.getJSONArray("fields");
                            JSONObject conditionJSON = transactionNodeJSON.getJSONObject("conditions");
                            List<Field> fieldList = JSON.parseArray(fieldJSON.toJSONString(), Field.class);
                            Condition condition = conditionJSON.toJavaObject(Condition.class);
                            BaseTransactionTemplate updateNode = new MysqlUpdateTransaction(oi, fieldList, condition);
                            list.add(updateNode);
                        } else if (StringUtil.isValid(type) && type.equalsIgnoreCase("delete")) {
                            //delete
                            if (!transactionNodeJSON.containsKey("fields")) {
                                //没有field字段
                                throw new LegoException("fields缺失", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
                            }
                            if (!transactionNodeJSON.containsKey("conditions")) {
                                //没有field字段
                                throw new LegoException("fields缺失", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
                            }
                            JSONObject conditionJSON = transactionNodeJSON.getJSONObject("conditions");
                            Condition condition = conditionJSON.toJavaObject(Condition.class);
                            BaseTransactionTemplate deleteNode = new MysqlDeleteTransaction(oi, condition);
                            list.add(deleteNode);
                        }
                    }
                } else {
                    //抛出异常
                    throw new LegoException("没有可执行的transaction对象", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
                }
            } else {

            }
            //获取DS
            DS ds = dsManager.getDSByAlias(StringUtil.getString(dsAlias.getFieldValue()));
            if (ds == null) {
                throw new LegoException("没有可执行的ds对象", ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
            }
            mysqlDataManager.executeTransaction(ds, list);
        } catch (DBException e) {
            e.printStackTrace();
            throw new LegoException(e.getMessage(), ERROR_LEGO_TRANSACTION_EXECUTE_ERROR);
        }


    }
}
