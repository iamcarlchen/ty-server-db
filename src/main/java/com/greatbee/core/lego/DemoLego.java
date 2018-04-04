package com.greatbee.core.lego;

import com.greatbee.base.util.StringUtil;
import com.greatbee.core.bean.server.InputField;
import org.springframework.stereotype.Component;

/**
 * Created by usagizhang on 18/4/3.
 */
@Component("demoLego")
public class DemoLego implements Lego {



    @Override
    public void execute(Input input, Output output) throws LegoException {
        InputField usernameField = input.getInputField("username");
        InputField passwordField = input.getInputField("password");

        usernameField.setFieldValue(StringUtil.getString(usernameField.getFieldValue()).toUpperCase());
        passwordField.setFieldValue(StringUtil.getString(passwordField.getFieldValue()).toUpperCase());

        output.setOutputValue("usernameField", usernameField.getFieldValue());
        output.setOutputValue("passwordField", passwordField.getFieldValue());
    }
}
