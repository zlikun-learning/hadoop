package com.zlikun.learning.service;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/9 21:38
 */
public class HelloServiceImpl implements HelloService {

    @Override
    public String say(String message) {
        System.out.println("call HelloService#say(String)");
        return "say:" + message;
    }

}
