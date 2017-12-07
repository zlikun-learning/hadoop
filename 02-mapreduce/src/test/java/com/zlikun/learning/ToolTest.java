package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.util.Map;

/**
 * 打印Tool的Configuration对象中所有属性的键-值对
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-04 10:20
 */
public class ToolTest {

    @Test
    public void test() throws Exception {
        ConfigurationPrinter.main(null);
    }

    static class ConfigurationPrinter extends Configured implements Tool {
        static {
            Configuration.addDefaultResource("configuration-1.xml");
        }
        @Override
        public int run(String[] strings) throws Exception {
            Configuration conf = getConf();
            for (Map.Entry<String, String> entry : conf) {
                System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
            }
            return 0;
        }

        public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
            System.out.println(exitCode);
        }
    }

}
