package com.zlikun.learning.login;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-26 13:49
 */
public class LoginLogParserTest {

    private LoginLogParser parser = new LoginLogParser();

    @Test
    public void parse() {

        parser.parse("8077,162047547,ANDROID_STUDENT_STORE,2016-10-25,2016-10-25 17:37:04.0");
        assertTrue(parser.isValid());
        assertEquals(ClientType.ANDROID, parser.getClient());

    }
}