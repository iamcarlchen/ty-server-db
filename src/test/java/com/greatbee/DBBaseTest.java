package com.greatbee;

import com.alibaba.fastjson.JSONObject;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

// import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Author: CarlChen
 * Date: 2017/11/21
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:test_server.xml")
public class DBBaseTest extends AbstractJUnit4SpringContextTests {
    protected ApplicationContext context;

    /**
     * @return
     */
    public String getServerConfigName() {
        return "test_server.xml";
    }

    /**
     * Set Up
     */
    public void setUp(String configName) {
        context = new ClassPathXmlApplicationContext(configName);
    }

    /**
     * Test Context
     */
    public void testContext() {
        System.out.println(context);
    }


    /**
     * 输出json数据
     *
     * @param o
     */
    protected void printJSONObject(Object o) {
        System.out.println(JSONObject.toJSONString(o));
    }


}
