package com.greatbee.core.db.mysql.testcase;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.DiffItem;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.db.mysql.MysqlSchemaDataManagerTest;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.mapping.Array;
import org.junit.Test;

public class TestDiff extends MysqlSchemaDataManagerTest implements ExceptionCode {

    DSView testDSView;
    DSView dbDSView;

    @Test
    public void testDiffByDeffault() throws DBException {
        //初始化
        this.setUp();
        this.initDSView();
        //执行测试用例
        List<DiffItem> diffItems = this.mysqlDataManager.diff(this.dbDSView);
        String result = JSONArray.toJSONString(diffItems);
        System.out.print(result);
    }

    public void initDSView() {

        dbDSView = new DSView();
        dbDSView.setDs(this.getDS());
        List<OIView> oiViews = new ArrayList<OIView>();

        //创建OI ly_article_category
        OIView ly_article_category_view = new OIView();
        ly_article_category_view.setOi(this.initOI("资讯分类", "ly_article_category", "ly_article_category"));
        List<Field> fields = new ArrayList<Field>();
        fields.add(initField(ly_article_category_view.getOi(), "id", "id", DT.INT, 11));
        fields.add(initField(ly_article_category_view.getOi(), "categoryId", "categoryId", DT.String, 64));
        fields.add(initField(ly_article_category_view.getOi(), "parentId", "parentId", DT.String, 64));
        fields.add(initField(ly_article_category_view.getOi(), "name", "name", DT.String, 32));
        fields.add(initField(ly_article_category_view.getOi(), "remark", "remark", DT.String, 256));
        fields.add(initField(ly_article_category_view.getOi(), "enable", "enable", DT.Boolean, 256));
        fields.add(initField(ly_article_category_view.getOi(), "sortNum", "sortNum", DT.INT, 11));
        fields.add(initField(ly_article_category_view.getOi(), "createEmployeeCode", "createEmployeeCode", DT.INT, 16));
        fields.add(initField(ly_article_category_view.getOi(), "createEmployeeName", "createEmployeeName", DT.INT, 32));
        fields.add(initField(ly_article_category_view.getOi(), "createDatetime", "createDatetime", DT.Time, 32));
        fields.add(initField(ly_article_category_view.getOi(), "updateEmployeeCode", "updateEmployeeCode", DT.INT, 16));
        fields.add(initField(ly_article_category_view.getOi(), "updateEmployeeName", "updateEmployeeName", DT.INT, 32));
        fields.add(initField(ly_article_category_view.getOi(), "updateDatetime", "updateDatetime", DT.Time, 32));
        fields.add(initField(ly_article_category_view.getOi(), "delete", "delete", DT.INT, 4));
        ly_article_category_view.setFields(fields);
        oiViews.add(ly_article_category_view);

        //创建OI ly_article_detail
        OIView ly_article_detail = new OIView();
        ly_article_detail.setOi(this.initOI("资讯分类", "ly_article_category", "ly_article_category"));
        List<Field> ly_article_detail_fields = new ArrayList<Field>();
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "id", "id", DT.INT, 11));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "categoryId", "categoryId", DT.String, 64));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "title", "title", DT.String, 128));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "summary", "summary", DT.String, 256));

        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "content", "content", DT.String, 256));

        // ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "cover", "cover", DT.Boolean, 2048));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "cover111", "cover111", DT.Boolean, 2048));

        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "likeCount", "likeCount", DT.INT, 11));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "commentCount", "commentCount", DT.INT, 11));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "shareCount", "shareCount", DT.INT, 11));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "readCount", "readCount", DT.INT, 11));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "isSendPush", "isSendPush", DT.INT, 4));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "sendDatetime", "sendDatetime", DT.Time, 4));

        // ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "remark", "remark", DT.String, 256));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "remark", "remark", DT.String, 16));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "targetType", "targetType", DT.String, 32));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "targetCode", "targetCode", DT.String, 1024));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "targetName", "targetName", DT.String, 2048));
        // ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "enable", "enable", DT.INT, 4));
        // ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "sortNum", "sortNum", DT.INT, 11));

        ly_article_detail_fields
                .add(initField(ly_article_detail.getOi(), "publishFrom", "publishFrom", DT.String, 128));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "pushStatus", "pushStatus", DT.INT, 4));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "shareStatus", "shareStatus", DT.INT, 4));

        ly_article_detail_fields
                .add(initField(ly_article_detail.getOi(), "createEmployeeCode", "createEmployeeCode", DT.INT, 16));
        ly_article_detail_fields
                .add(initField(ly_article_detail.getOi(), "createEmployeeName", "createEmployeeName", DT.INT, 32));
        ly_article_detail_fields
                .add(initField(ly_article_detail.getOi(), "createDatetime", "createDatetime", DT.Time, 32));
        ly_article_detail_fields
                .add(initField(ly_article_detail.getOi(), "updateEmployeeCode", "updateEmployeeCode", DT.INT, 16));
        ly_article_detail_fields
                .add(initField(ly_article_detail.getOi(), "updateEmployeeName", "updateEmployeeName", DT.INT, 32));
        ly_article_detail_fields
                .add(initField(ly_article_detail.getOi(), "updateDatetime", "updateDatetime", DT.Time, 32));
        ly_article_detail_fields.add(initField(ly_article_detail.getOi(), "delete", "delete", DT.INT, 4));
        ly_article_detail.setFields(ly_article_detail_fields);
        oiViews.add(ly_article_detail);

        dbDSView.setOiViews(oiViews);

        testDSView = new DSView();
    }

}