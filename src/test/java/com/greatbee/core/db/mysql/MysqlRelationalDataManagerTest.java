
package com.greatbee.core.db.mysql;

import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DST;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.db.RelationalDataManager;
import com.greatbee.core.db.SchemaDataManager;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.util.DataSourceUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试 Mysql Schema Data Manager
 */
public abstract class MysqlRelationalDataManagerTest extends MysqlDataManagerTest implements ExceptionCode {

    public RelationalDataManager mysqlDataManager;

    protected OIView mainView;
    protected OIView subView;

    /**
     * setUp 设置测试用例
     */
    public void setUp() {
        super.setUp();

        //加载manager
        mysqlDataManager = (RelationalDataManager) context.getBean("mysqlDataManager");
    }

    public void settingOIView() {
        List<Field> mainViewFields = new ArrayList<>();
        mainView = new OIView();
        mainView.setOi(this.initOI("文章", "ly_article_detail", "ly_article_detail"));
        mainViewFields.add(this.initField(mainView.getOi(), "id", "id", DT.INT, 11, true));
        mainViewFields.add(this.initField(mainView.getOi(), "categoryId", "categoryId", DT.String, 64));
        mainViewFields.add(this.initField(mainView.getOi(), "title", "title", DT.String, 128));
        mainViewFields.add(this.initField(mainView.getOi(), "summary", "summary", DT.String, 256));
        mainViewFields.add(this.initField(mainView.getOi(), "content", "content", DT.String, 2048));
        mainViewFields.add(this.initField(mainView.getOi(), "cover", "cover", DT.String, 2048));
        mainViewFields.add(this.initField(mainView.getOi(), "likeCount", "likeCount", DT.INT, 11));
        mainViewFields.add(this.initField(mainView.getOi(), "commentCount", "commentCount", DT.INT, 11));
        mainViewFields.add(this.initField(mainView.getOi(), "shareCount", "shareCount", DT.INT, 11));
        mainViewFields.add(this.initField(mainView.getOi(), "readCount", "readCount", DT.INT, 11));
        mainViewFields.add(this.initField(mainView.getOi(), "isSendPush", "isSendPush", DT.Boolean, 11));
        mainViewFields.add(this.initField(mainView.getOi(), "sendDatetime", "sendDatetime", DT.Time, 11));
        mainViewFields.add(this.initField(mainView.getOi(), "remark", "remark", DT.String, 256));
        mainViewFields.add(this.initField(mainView.getOi(), "targetType", "targetType", DT.String, 32));
        mainViewFields.add(this.initField(mainView.getOi(), "targetCode", "targetCode", DT.String, 1024));
        mainViewFields.add(this.initField(mainView.getOi(), "targetName", "targetName", DT.String, 2048));
        mainViewFields.add(this.initField(mainView.getOi(), "enable", "enable", DT.Boolean, 1));
        mainViewFields.add(this.initField(mainView.getOi(), "sortNum", "sortNum", DT.INT, 11));
        mainViewFields.add(this.initField(mainView.getOi(), "createEmployeeCode", "createEmployeeCode", DT.String, 16));
        mainViewFields.add(this.initField(mainView.getOi(), "createEmployeeName", "createEmployeeName", DT.String, 32));
        mainViewFields.add(this.initField(mainView.getOi(), "updateEmployeeCode", "updateEmployeeCode", DT.String, 16));
        mainViewFields.add(this.initField(mainView.getOi(), "updateEmployeeName", "updateEmployeeName", DT.String, 32));
        mainViewFields.add(this.initField(mainView.getOi(), "updateDatetime", "updateDatetime", DT.Time, 32));
        mainViewFields.add(this.initField(mainView.getOi(), "publishFrom", "publishFrom", DT.String, 128));
        mainViewFields.add(this.initField(mainView.getOi(), "createDatetime", "createDatetime", DT.Time, 32));
        mainViewFields.add(this.initField(mainView.getOi(), "pushStatus", "pushStatus", DT.Boolean, 1));
        mainViewFields.add(this.initField(mainView.getOi(), "shareStatus", "shareStatus", DT.Boolean, 1));
        mainViewFields.add(this.initField(mainView.getOi(), "serialNumber", "serialNumber", DT.String, 128));

        mainView.setFields(mainViewFields);

        List<Field> subViewFields = new ArrayList<>();
        subView = new OIView();
        subView.setOi(this.initOI("文章分类", "ly_article_category", "ly_article_category"));
        subViewFields.add(this.initField(subView.getOi(), "id", "id", DT.INT, 11, true));
        subViewFields.add(this.initField(subView.getOi(), "categoryId", "categoryId", DT.String, 64));
        subViewFields.add(this.initField(subView.getOi(), "parentId", "parentId", DT.String, 64));
        subViewFields.add(this.initField(subView.getOi(), "name", "name", DT.String, 32));
        subViewFields.add(this.initField(subView.getOi(), "remark", "remark", DT.String, 256));
        subViewFields.add(this.initField(subView.getOi(), "enable", "enable", DT.Boolean, 1));
        subViewFields.add(this.initField(subView.getOi(), "sortNum", "sortNum", DT.INT, 11));
        subViewFields.add(this.initField(subView.getOi(), "createEmployeeCode", "createEmployeeCode", DT.String, 16));
        subViewFields.add(this.initField(subView.getOi(), "createEmployeeName", "createEmployeeName", DT.String, 32));
        subViewFields.add(this.initField(subView.getOi(), "updateEmployeeCode", "updateEmployeeCode", DT.String, 16));
        subViewFields.add(this.initField(subView.getOi(), "updateEmployeeName", "updateEmployeeName", DT.String, 32));
        subViewFields.add(this.initField(subView.getOi(), "updateDatetime", "updateDatetime", DT.Time, 32));
        subViewFields.add(this.initField(subView.getOi(), "createDatetime", "createDatetime", DT.Time, 32));

        subView.setFields(subViewFields);
    }

    public void executeQuery(Connection conn, PreparedStatement ps, String querySql) throws SQLException {
        ps = conn.prepareStatement(querySql);
        ps.execute();
    }
}