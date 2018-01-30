package com.zlikun.learning.helper;

import org.junit.Test;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 17:12
 */
public class StringFormatTest {

    @Test
    public void test() {

        // 输出12位字符串，左边补0
        // 000000012306
        System.out.println(String.format("%012d", 12306));

        // 输出一个RowKey
        // 001517306026000000000001
        System.out.println(String.format("%012d", System.currentTimeMillis() / 1000) + String.format("%012d", 1L));

    }

}
