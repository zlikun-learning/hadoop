package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-04 09:45
 */
public class ConfigurationTest {

    @Test
    public void test() {

        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");

        // 测试属性读取
        assertThat(conf.get("color"), is("yellow"));
        assertThat(conf.getInt("size", 0), is(10));
        assertThat(conf.get("breadth", "wide"), is("wide"));

        assertThat(conf.get("size-weight"), is("10,heavy"));
        // 系统属性优先级高于资源文件中定义的属性
        // 需要注意的是，如果资源文件中未定义，使用系统属性定义的变量，是无法通过配置API来访问
        System.setProperty("size", "14");
        assertThat(conf.get("size-weight"), is("14,heavy"));

        // 测试属性覆盖
        conf.addResource("configuration-2.xml");
        // 属性发生了覆盖
        assertThat(conf.getInt("size", 0), is(12));
        // 被标记为`final`，所以无法覆盖
        assertThat(conf.get("weight"), is("heavy"));

    }

}
