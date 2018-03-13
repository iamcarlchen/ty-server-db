package com.greatbee.core.lego;

import com.greatbee.core.ExceptionCode;
import org.springframework.stereotype.Component;

/**
 * Created by usagizhang on 18/3/13.
 */
@Component("dataBaseTransaction")
public class DataBaseTransaction implements Lego, ExceptionCode {
    @Override
    public void execute(Input input, Output output) throws LegoException {



    }
}
