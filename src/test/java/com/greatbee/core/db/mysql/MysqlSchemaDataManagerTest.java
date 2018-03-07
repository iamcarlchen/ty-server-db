
package com.greatbee.core.db.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DST;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.SchemaDataManager;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.util.DataSourceUtils;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * 测试 Mysql Schema Data Manager
 */
public abstract class MysqlSchemaDataManagerTest extends DBBaseTest implements ExceptionCode {

    private String testConnectionUrl = "jdbc:mysql://localhost:3306/db_ty_test?useUnicode=true&characterEncoding=utf8";
    private String testConnectionUsername = "root";
    private String testConnectionPassword = "";

    // @Autowired
    private DSManager dsManager;
    // @Autowired
    public SchemaDataManager mysqlDataManager;
    public Connection conn = null;
    public PreparedStatement ps = null;

    /**
     * setUp 设置测试用例
     */
    public void setUp() {
        super.setUp("test_server.xml");
        // super.setUp("ty_db_server.xml");
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

    public DS getDS() {
        DS _ds = new DS();
        _ds.setName("测试数据源");
        _ds.setAlias("test_mysql");
        _ds.setDst(DST.Mysql.getType());
        _ds.setConnectionUrl(testConnectionUrl);
        _ds.setConnectionUsername(testConnectionUsername);
        _ds.setConnectionPassword(testConnectionPassword);
        return _ds;
    }

    public void initConn() throws DBException {
        // DSView dsView = this.getDSView();
        // Connection conn = null;
        // PreparedStatement ps = null;
        try {
            //初始化数据库连接

            // this.ds = _ds;
            // System.out.println("ds alias = " + ds.getAlias());
            this.conn = DataSourceUtils.getDatasource(this.getDS()).getConnection();
            // this.ds = _ds;
            this.ps = null;
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

        //drop table
        this.executeQuery(conn, ps, "DROP TABLE IF EXISTS `ly_article_category`;");
        this.executeQuery(conn, ps, "DROP TABLE IF EXISTS `ly_article_detail`;");
        this.executeQuery(conn, ps, "DROP TABLE IF EXISTS `ly_base_app`;");

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

        this.executeQuery(conn, ps, queryBuilder.toString());

        //创建文章表
        queryBuilder = new StringBuilder();
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
        this.executeQuery(conn, ps, queryBuilder.toString());

        queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE `ly_base_app` (");
        queryBuilder.append("`appId` varchar(64) NOT NULL COMMENT '唯一不重复，自主填写',");
        queryBuilder.append("`appSecretKey` varchar(256) DEFAULT NULL COMMENT '访问秘钥',");
        queryBuilder.append("`appName` varchar(128) DEFAULT NULL COMMENT 'app名称',");
        queryBuilder.append("`enable` tinyint(4) DEFAULT NULL COMMENT '0=关闭，1=启用',");
        queryBuilder.append("`remark` varchar(256) DEFAULT NULL COMMENT '备注',");
        queryBuilder.append("`contacts` varchar(128) DEFAULT NULL COMMENT '应用联系人',");
        queryBuilder.append("`mainCharge` varchar(128) DEFAULT NULL COMMENT '应用负责人',");
        queryBuilder.append("`contactsMobilePhone` varchar(128) DEFAULT NULL COMMENT '联系人电话',");
        queryBuilder.append("`updateDate` datetime DEFAULT NULL COMMENT '更新时间',");
        queryBuilder.append("`updateEmployeeCode` varchar(32) DEFAULT NULL COMMENT '更新员工code',");
        queryBuilder.append("`updateEmployeeName` varchar(64) DEFAULT NULL COMMENT '更新员工姓名',");
        queryBuilder.append("`deleteFlg` tinyint(2) DEFAULT NULL COMMENT '删除标识',");
        queryBuilder.append("PRIMARY KEY (`appId`)");
        queryBuilder.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
        this.executeQuery(conn, ps, queryBuilder.toString());

        System.out.println("schema done!");
    }

    /**
     * 初始化测试数据
     */
    public void initTestData(Connection conn, PreparedStatement ps) throws DBException, SQLException {

        //插入文章分类数据
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_category` ( `categoryId`, `parentId`, `name`, `remark`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `createDatetime`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `delete`) VALUES ( 'zuixin', 'gonggao', '最新', 'null', '1', '1', '12345', '12345', '2017-01-01 00:00:00', '51029236', '龚子国', '2017-06-08 10:14:37', '0');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_category` ( `categoryId`, `parentId`, `name`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `createDatetime`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `delete`) VALUES ( 'renming', 'gonggao', '任命', '1', '2', '12345', '12345', '2017-01-01 00:00:00', '51032636', '董隽晶', '2017-04-25 15:00:27', '0');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_category` ( `categoryId`, `parentId`, `name`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `createDatetime`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `delete`) VALUES ( 'jiyao', 'gonggao', '纪要', '1', '3', '12345', '12345', '2017-01-01 00:00:00', '51032636', '董隽晶', '2017-04-25 14:49:23', '0');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_category` ( `categoryId`, `parentId`, `name`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `createDatetime`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `delete`) VALUES ( 'jiangcheng', 'gonggao', '奖惩', '1', '4', '12345', '12345', '2017-01-01 00:00:00', '51032636', 'null', '2017-04-24 16:29:09', '0');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_category` ( `categoryId`, `parentId`, `name`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `createDatetime`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `delete`) VALUES ( 'tongzhi', 'gonggao', '通知', '1', '5', '12345', '12345', '2017-01-01 00:00:00', '51029458', '张学超', '2017-04-25 15:17:17', '0');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_category` ( `categoryId`, `parentId`, `name`, `remark`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `createDatetime`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `delete`) VALUES ( '97C64AA6', 'ROOT', '职工之家', 'null', '1', '1', '51029458', '张学超', '2017-04-27 20:48:24', '51031417', '戴金辉', '2017-04-28 16:52:50', '0');");

        //插入文章数据
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_detail` ( `categoryId`, `title`, `summary`, `content`, `cover`, `likeCount`, `commentCount`, `shareCount`, `readCount`, `isSendPush`, `sendDatetime`, `remark`, `targetType`, `targetCode`, `targetName`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `publishFrom`, `createDatetime`, `shareStatus`, `serialNumber`) VALUES ( 'B9F01112', '商户联盟“唱”出新声，吸金2亿六店“合”肥', '商户联合营销新物种——同城盛典', '', 'http://img2.mklimg.com/g1/M00/0E/09/rBBrBlkk8diAUxL2AADVkp69i6U351.jpg', '0', '0', '0', '1', '0', '2017-05-25 10:53:53', '', 'All', '', '', '1', '1', '51031417', '戴金辉', '51031417', '戴金辉', '2017-05-24 17:20:47', '', '2017-05-24 09:53:53', '0', 'CD3925553F0996965A5A1723CE423790');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_detail` ( `categoryId`, `title`, `summary`, `content`, `cover`, `likeCount`, `commentCount`, `shareCount`, `readCount`, `isSendPush`, `sendDatetime`, `remark`, `targetType`, `targetCode`, `targetName`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `publishFrom`, `createDatetime`, `pushStatus`, `shareStatus`, `serialNumber`) VALUES ( '97C64AAA', '践行公益｜用爱心为生命加油！——集团总部员工撸起袖子无偿献血', '2017-05-24', '', 'http://img2.mklimg.com/g2/M00/0E/08/rBBrClkk6dKAaTI5AACBbQAU3_M577.jpg', '0', '0', '0', '2', '1', '2017-05-24 09:41:48', '', 'All', '', '', '1', '0', '51035039', '胡云江', '', '', '2017-05-24 10:04:17', '', '2017-05-24 10:04:17', '3', '0', '30E163A7B1A884E6B83184D969A432E8');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_detail` ( `categoryId`, `title`, `summary`, `content`, `cover`, `likeCount`, `commentCount`, `shareCount`, `readCount`, `isSendPush`, `sendDatetime`, `remark`, `targetType`, `targetCode`, `targetName`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `publishFrom`, `createDatetime`, `pushStatus`, `shareStatus`, `serialNumber`) VALUES ( 'B9F01112', '设计师带客包场，4小时刷卡近3000万', '今晚吃火锅的', '', 'http://img1.mklimg.com/g2/M00/0E/10/rBBrClklOYuAG-p9AADYF56VFJI225.jpg', '0', '0', '0', '3', '1', '2017-05-25 17:14:00', '', 'All', '', '', '1', '0', '51031417', '戴金辉', '51031417', '戴金辉', '2017-05-25 17:08:59', '', '2017-05-24 10:52:39', '0', '0', '51CE02264CB8D85419BB018FA4DCD80C');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_detail` ( `categoryId`, `title`, `summary`, `content`, `cover`, `likeCount`, `commentCount`, `shareCount`, `readCount`, `isSendPush`, `sendDatetime`, `remark`, `targetType`, `targetCode`, `targetName`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `publishFrom`, `createDatetime`, `shareStatus`, `serialNumber`) VALUES ( 'BD70E74D', '融融端午情，红星家万兴，集团工会祝全体红星人节日快乐！', '2017-05-24', '', 'http://img2.mklimg.com/g2/M00/0E/0C/rBBrClklFyyAAdXtAABgdgAgde8955.jpg', '0', '0', '0', '0', '1', '2017-05-24 13:13:19', '', 'Department', '10000158', '集团总部-党工团-工会', '1', '0', '51035039', '胡云江', '51035039', '胡云江', '2017-05-24 16:34:12', '', '2017-05-24 13:17:08', '0', '529F4B2DC5F6E77BFC7E0BD378A676FB');");
        this.executeQuery(conn, ps,
                "INSERT INTO `ly_article_detail` ( `categoryId`, `title`, `summary`, `content`, `cover`, `likeCount`, `commentCount`, `shareCount`, `readCount`, `isSendPush`, `sendDatetime`, `remark`, `targetType`, `targetCode`, `targetName`, `enable`, `sortNum`, `createEmployeeCode`, `createEmployeeName`, `updateEmployeeCode`, `updateEmployeeName`, `updateDatetime`, `publishFrom`, `createDatetime`, `shareStatus`, `serialNumber`) VALUES ( 'B9F01112', '人人都在转H5，只有她转出了RMB', '一样的朋友圈，一样的导购员，一样的H5，为啥业绩不一样？', '', 'http://img1.mklimg.com/g2/M00/0E/13/rBBrCVklTimAebk6AACAql7_oQY322.jpg', '0', '0', '0', '0', '0', '2017-05-24 15:47:52', '', 'All', '', '', '1', '0', '51031417', '戴金辉', '51031417', '戴金辉', '2017-05-24 17:18:51', '', '2017-05-24 15:47:52', '0', 'F30577EF6769A9CF4E91D96B0AB222F5');");

    }

    public void executeQuery(Connection conn, PreparedStatement ps, String querySql) throws SQLException {
        ps = conn.prepareStatement(querySql);
        ps.execute();
    }

    /**
     * 初始化OI
     */
    protected OI initOI(String name, String alias, String resource) {
        OI oi = new OI();
        oi.setDsAlias(this.getDS().getAlias());
        oi.setAlias(alias);
        oi.setName(name);
        oi.setResource(resource);

        return oi;
    }

    /**
     * 初始化字段
     */
    protected Field initField(OI oi, String name, String fieldName, DT dt, int fieldLength) {
        return this.initField(oi, name, fieldName, dt, fieldLength, false);
    }

    /**
     * 初始化字段
     */
    protected Field initField(OI oi, String name, String fieldName, DT dt, int fieldLength, boolean isPK) {
        Field fId = new Field();
        fId.setDt(dt.getType());
        fId.setName(name);
        fId.setFieldName(fieldName);
        fId.setOiAlias(oi.getAlias());
        fId.setFieldLength(fieldLength);
        fId.setPk(isPK);
        return fId;
    }

    // public abstract DS getDS() throws DBException;
    // public abstract DSView getDsView() throws DBException;

    

    // //updateField 更新字段名称
    // public void testUpdateField() throws DBException {

    // }

    // //updateField 不更新字段名称
    // public void testUpdateFieldWithoutName() throws DBException {

    // }

}