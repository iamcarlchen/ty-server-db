
package com.greatbee.core.db.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DST;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.db.SchemaDataManager;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.util.DataSourceUtils;

/**
 * 测试 Mysql Schema Data Manager
 */
public abstract class MysqlSchemaDataManagerTest extends DBBaseTest implements ExceptionCode {

    private String testConnectionUrl = "jdbc:mysql://localhost:3306/db_ty_test?useUnicode=true&characterEncoding=utf8";
    private String testConnectionUsername = "root";
    private String testConnectionPassword = "";

    private DSManager dsManager;
    private SchemaDataManager mysqlDataManager;
    private Connection conn = null;
    private PreparedStatement ps = null;

    /**
     * setUp 设置测试用例
     */
    public void setUp() {
        super.setUp("test_server.xml");
        super.setUp("ty_db_server.xml");
        //加载manager
        dsManager = (DSManager) context.getBean("dsManager");
        mysqlDataManager = (SchemaDataManager) context.getBean("mysqlDataManager");
        try {
            //初始化测试数据
            this.initConn();
            this.initTestSchema(conn, ps);
            this.releaseConn();
        } catch (DBException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // @Override
    // public void tearDown() throws Exception {
    //     super.tearDown();
    // }

    public void initConn() throws DBException {
        // DSView dsView = this.getDSView();
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            //初始化数据库连接
            DS ds = new DS();
            ds.setName("测试数据源");
            ds.setAlias("test_mysql_datasource");
            ds.setDst(DST.Mysql.getType());
            ds.setConnectionUrl(testConnectionUrl);
            ds.setConnectionUsername(testConnectionUsername);
            ds.setConnectionPassword(testConnectionPassword);
            conn = DataSourceUtils.getDatasource(ds).getConnection();
            ps = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void releaseConn() throws DBException {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭PreparedStatement错误", ERROR_DB_PS_CLOSE_ERROR);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new DBException("关闭connection错误", ERROR_DB_CONN_CLOSE_ERROR);
            }
        }
    }

    /**
     * 初始化schema
     */
    public void initTestSchema(Connection conn, PreparedStatement ps) throws DBException, SQLException {
        this.initTestTable(conn, ps);
        this.initTestData(conn, ps);
    }

    /**
     * 初始化测试表
     */
    public void initTestTable(Connection conn, PreparedStatement ps) throws DBException, SQLException {
        StringBuilder queryBuilder = new StringBuilder();
        //创建文章分类表
        queryBuilder.append("CREATE TABLE `ly_article_category` (");
        queryBuilder.append("`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增长，无意义',");
        queryBuilder.append("`categoryId` varchar(64) DEFAULT NULL COMMENT '分类ID，不能重复',");
        queryBuilder.append("`parentId` varchar(64) DEFAULT NULL COMMENT '父级分类的categoryId，ROOT标示是一级分类。',");
        queryBuilder.append("`name` varchar(32) DEFAULT NULL COMMENT '分类名称',");
        queryBuilder.append("`remark` varchar(256) DEFAULT NULL COMMENT '备注',");
        queryBuilder.append("`enable` tinyint(4) DEFAULT NULL COMMENT '1=启用，0=禁用',");
        queryBuilder.append("`sortNum` int(11) DEFAULT NULL COMMENT '排序ID，越大的排越后面',");
        queryBuilder.append("`createEmployeeCode` varchar(16) DEFAULT NULL COMMENT '创建人工号 ',");
        queryBuilder.append("`createEmployeeName` varchar(32) DEFAULT NULL COMMENT '创建人姓名',");
        queryBuilder.append("`createDatetime` datetime DEFAULT NULL COMMENT '创建时间',");
        queryBuilder.append("`updateEmployeeCode` varchar(16) DEFAULT NULL COMMENT '更新人工号',");
        queryBuilder.append("`updateEmployeeName` varchar(32) DEFAULT NULL COMMENT '更新人姓名   ',");
        queryBuilder.append("`updateDatetime` datetime DEFAULT NULL,");
        queryBuilder.append("`delete` tinyint(4) DEFAULT NULL COMMENT '0=未删除，1=已删除',");
        queryBuilder.append("PRIMARY KEY (`id`)");
        queryBuilder.append(") ENGINE=InnoDB  DEFAULT CHARSET=utf8;");

        //创建文章表
        queryBuilder.append("CREATE TABLE `ly_article_detail` (");
        queryBuilder.append("`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增长ID',");
        queryBuilder.append("`categoryId` varchar(64) DEFAULT NULL COMMENT '分类ID，对应分类表的categoryId',");
        queryBuilder.append("`title` varchar(128) DEFAULT NULL COMMENT '文章标题',");
        queryBuilder.append("`summary` varchar(256) DEFAULT NULL COMMENT '摘要',");
        queryBuilder.append("`content` longtext COMMENT '正文内容  ',");
        queryBuilder.append("`cover` varchar(2048) DEFAULT NULL,");
        queryBuilder.append("`likeCount` int(11) DEFAULT NULL COMMENT '点赞数',");
        queryBuilder.append("`commentCount` int(11) DEFAULT NULL COMMENT '评论数',");
        queryBuilder.append("`shareCount` int(11) DEFAULT NULL COMMENT '分享数',");
        queryBuilder.append("`readCount` int(11) DEFAULT NULL COMMENT '阅读数',");
        queryBuilder.append("`isSendPush` tinyint(4) DEFAULT NULL COMMENT '是否发送推送,0=不发送，1=发送',");
        queryBuilder.append("`sendDatetime` datetime DEFAULT NULL COMMENT '发送推送的时间',");
        queryBuilder.append("`remark` varchar(256) DEFAULT NULL COMMENT '备注',");
        queryBuilder.append("`targetType` varchar(32) DEFAULT NULL COMMENT '发送目标类型，Department=组织架构，Employee=员工',");
        queryBuilder.append("`targetCode` varchar(1024) DEFAULT NULL COMMENT '发送的目标Code（组织架构Code或员工Code）,逗号间隔',");
        queryBuilder.append("`targetName` varchar(2048) DEFAULT NULL COMMENT '发送的目标名称（组织架构名称或员工姓名）,逗号间隔',");
        queryBuilder.append("`enable` tinyint(4) DEFAULT NULL COMMENT '是否启用，1=启用，0=禁用',");
        queryBuilder.append("`sortNum` int(11) DEFAULT NULL COMMENT '排序ID，越大的排越后面',");
        queryBuilder.append("`createEmployeeCode` varchar(16) DEFAULT NULL COMMENT '创建人工号   ',");
        queryBuilder.append("`createEmployeeName` varchar(32) DEFAULT NULL COMMENT '创建人姓名',");
        queryBuilder.append("`updateEmployeeCode` varchar(16) DEFAULT NULL COMMENT '更新人工号',");
        queryBuilder.append("`updateEmployeeName` varchar(32) DEFAULT NULL COMMENT '更新人姓名  ',");
        queryBuilder.append("`updateDatetime` datetime DEFAULT NULL COMMENT '更新时间',");
        queryBuilder.append("`publishFrom` varchar(128) DEFAULT NULL COMMENT '发布方',");
        queryBuilder.append("`createDatetime` datetime DEFAULT NULL COMMENT '创建时间',");
        queryBuilder.append("`pushStatus` tinyint(4) DEFAULT NULL COMMENT '推送状态，0=不推送，1=等待推送，2=正在推送，3=推送完毕，-1=推送失败',");
        queryBuilder.append("`shareStatus` tinyint(4) DEFAULT NULL COMMENT '分享状态 0=关闭分享，1=开启分享\n',");
        queryBuilder.append("`serialNumber` varchar(128) DEFAULT NULL COMMENT '序列号，随机生成',");
        queryBuilder.append("PRIMARY KEY (`id`)");
        queryBuilder.append(") ENGINE=InnoDB  DEFAULT CHARSET=utf8;");

        ps = conn.prepareStatement(queryBuilder.toString());
        ps.execute();
        System.out.println("schema done!");
    }

    /**
     * 初始化测试数据
     */
    public void initTestData(Connection conn, PreparedStatement ps) throws DBException {

    }

    // public abstract DS getDS() throws DBException;
    // public abstract DSView getDsView() throws DBException;

    //diff
    // public void testDiff() throws DBException {

    // }

    // //createTable
    // public void testCreateTable() throws DBException {

    // }

    // //dropTable
    // public void testDropTable() throws DBException {

    // }

    // //addField
    // public void testAddField() throws DBException {

    // }

    // //dropField
    // public void testDropField() throws DBException {

    // }

    // //updateField 更新字段名称
    // public void testUpdateField() throws DBException {

    // }

    // //updateField 不更新字段名称
    // public void testUpdateFieldWithoutName() throws DBException {

    // }

}