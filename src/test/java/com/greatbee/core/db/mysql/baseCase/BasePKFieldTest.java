package com.greatbee.core.db.mysql.baseCase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.db.mysql.MysqlRelationalDataManagerTest;
import org.junit.Test;

/**
 * Created by usagizhang on 18/3/14.
 */
public abstract class BasePKFieldTest extends MysqlRelationalDataManagerTest implements ExceptionCode {


    protected Field pk1;
    protected Field pk2;
    protected Field pk3;
    protected Field pk4;
    protected Field pk5;

    public BasePKFieldTest() throws DBException {
        this.setUp();
        this.settingOIView();
        this.initPKField();
    }

    @Test
    public void startTest() throws DBException {
        this.runTest(pk1);
        this.runTest(pk2);
        this.runTest(pk3);
        this.runTest(pk4);
        this.runTest(pk5);
    }

    protected abstract void runTest(Field pkField) throws DBException;

    public void initPKField() {
        pk1 = this.initField(mainView.getOi(), "id", "id", DT.INT, 11, true);
        pk1.setFieldValue("1");
        pk2 = this.initField(mainView.getOi(), "id", "id", DT.INT, 11, true);
        pk2.setFieldValue("2");
        pk3 = this.initField(mainView.getOi(), "id", "id", DT.INT, 11, true);
        pk3.setFieldValue("3");
        pk4 = this.initField(mainView.getOi(), "id", "id", DT.INT, 11, true);
        pk4.setFieldValue("4");
        pk5 = this.initField(mainView.getOi(), "id", "id", DT.INT, 11, true);
        pk5.setFieldValue("5");
    }


}
