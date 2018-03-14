package com.greatbee.core.db.mysql.baseCase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.db.mysql.MysqlRelationalDataManagerTest;
import org.junit.Test;

import java.util.List;

/**
 * Created by usagizhang on 18/3/14.
 */
public abstract class BaseConditionTest extends MysqlRelationalDataManagerTest implements ExceptionCode {


    protected Condition eqCondition;
    protected Condition gtCondition;
    protected Condition geCondition;
    protected Condition likeCondition;

    public BaseConditionTest() throws DBException {
        this.setUp();
        this.settingOIView();
        this.initCondition();
    }

    @Test
    public void startTest() throws DBException {
        this.runTest(eqCondition);
//        this.runTest(gtCondition);
//        this.runTest(geCondition);
//        this.runTest(likeCondition);
    }

    protected abstract void runTest(Condition queryCondition) throws DBException;

    public void initCondition() {
        List<Field> fields = mainView.getFields();
        eqCondition = new Condition();
        eqCondition.setConditionFieldName("categoryId");
        eqCondition.setConditionFieldValue("B9F01112");
        eqCondition.setCt(CT.EQ.getName());


        gtCondition = new Condition();
        gtCondition.setConditionFieldName("id");
        gtCondition.setConditionFieldValue("3");
        gtCondition.setCt(CT.GT.getName());

        geCondition = new Condition();
        geCondition.setConditionFieldName("id");
        geCondition.setConditionFieldValue("3");
        geCondition.setCt(CT.GE.getName());


        likeCondition= new Condition();
        likeCondition.setConditionFieldName("title");
        likeCondition.setConditionFieldValue("人人");
        likeCondition.setCt(CT.LIKE.getName());

    }


}
