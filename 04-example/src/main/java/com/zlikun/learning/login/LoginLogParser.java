package com.zlikun.learning.login;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateUtils;

import java.text.ParseException;
import java.util.Date;

/**
 * 登录日志解析器
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-26 11:20
 */
public class LoginLogParser {

    private Long userId ;
    private ClientType client ;
    private Date loginTime;

    public void parse(String message) {
        if (StringUtils.isBlank(message)) return;
        String [] array = message.split(",");
        if (array == null || array.length != 5) return;
        userId = NumberUtils.toLong(array[1]);
        if (array[2].toUpperCase().equals("PC_WEB")) {
            client = ClientType.PC;
        } else if (array[2].toUpperCase().startsWith("ANDROID_")) {
            client = ClientType.ANDROID;
        } else if (array[2].toUpperCase().startsWith("IOS_")) {
            client = ClientType.IOS;
        }
        try {
            loginTime = DateUtils.parseDate(array[4], new String [] {"yyyy-MM-dd HH:mm:ss.0"});
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public boolean isValid() {
        if (userId == null) return false;
        if (client == null) return false;
        if (loginTime == null) return false;
        return true;
    }

    public Long getUserId() {
        return userId;
    }

    public ClientType getClient() {
        return client;
    }

    public Date getLoginTime() {
        return loginTime;
    }
}
