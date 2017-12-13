package com.greatbee;

import junit.framework.TestCase;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Author: CarlChen
 * Date: 2017/11/21
 */
public class DBBaseTest extends TestCase {
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


}
