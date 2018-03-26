package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.DataUtil;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.db.mysql.MysqlRelationalDataManagerTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by usagizhang on 18/3/14.
 */
public class TestCreate extends MysqlRelationalDataManagerTest {


    @Test
    public void testMain() throws DBException {
        this.setUp();
        this.settingOIView();

        List<Field> addFields = new ArrayList<>();
        addFields.add(this.initField(mainView.getOi(), "categoryId", "categoryId", "123", DT.String, 256, false));
        addFields.add(this.initField(mainView.getOi(), "title", "title", "测试标题", DT.String, 256, false));
        addFields.add(this.initField(mainView.getOi(), "summary", "summary", "测试摘要", DT.String, 256, false));
        addFields.add(this.initField(mainView.getOi(), "content", "content", "测试正文", DT.String, 256, false));
        addFields.add(this.initField(mainView.getOi(), "likeCount", "likeCount", "0", DT.INT, 11, false));
        addFields.add(this.initField(mainView.getOi(), "commentCount", "commentCount", "0", DT.INT, 11, false));
        addFields.add(this.initField(mainView.getOi(), "shareCount", "shareCount", "0", DT.INT, 11, false));
        addFields.add(this.initField(mainView.getOi(), "readCount", "readCount", "0", DT.INT, 11, false));
        addFields.add(this.initField(mainView.getOi(), "isSendPush", "isSendPush", "0", DT.Boolean, 11, false));
        addFields.add(this.initField(mainView.getOi(), "sendDatetime", "sendDatetime", DataUtil.formatDate(new Date()), DT.Boolean, 11, false));

        this.mysqlDataManager.create(mainView.getOi(), addFields);
    }


}
