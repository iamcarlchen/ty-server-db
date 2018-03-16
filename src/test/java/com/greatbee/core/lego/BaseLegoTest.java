package com.greatbee.core.lego;

import com.greatbee.DBBaseTest;
import com.greatbee.core.ExceptionCode;

/**
 * DB 乐高测试用例基类
 * Created by usagizhang on 18/3/16.
 */
public class BaseLegoTest extends DBBaseTest implements ExceptionCode {

    public BaseLegoTest() {

    }

    /**
     * 初始化
     */
    public void setUp() {
        super.setUp("test_server.xml");
    }



}
