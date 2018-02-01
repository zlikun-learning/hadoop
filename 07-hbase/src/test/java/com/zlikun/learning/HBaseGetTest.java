package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * HBase查询API测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/14 14:37
 */
public class HBaseGetTest {

    private Configuration configuration;
    private Connection connection ;
    private Admin admin ;
    private TableName tableName;

    @Before
    public void init() throws IOException {
        this.configuration = HBaseConfiguration.create();
        // 设置master连接地址
        configuration.set("hbase.master","m4:16010");
        // 设置连接参数：HBase数据库所在的主机IP
        configuration.set("hbase.zookeeper.quorum", "m4");
        // 设置连接参数：HBase数据库使用的端口
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        this.connection = ConnectionFactory.createConnection(configuration);
        this.admin = connection.getAdmin();

        this.tableName = TableName.valueOf("user_login_logs");
    }

    @After
    public void destroy() throws IOException {
        connection.close();
        admin.close();
    }

    @Test
    public void get() throws IOException {
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes("001514334531000170753157"));
        Result result = table.get(get);

        final byte [] family = Bytes.toBytes("user");

        // 返回的都是字节数组，根据原类型转换需要的类型
        // 字符串类型
        assertEquals("MOBILE", Bytes.toString(result.getValue(family, Bytes.toBytes("accountType"))));
        assertEquals("222.66.154.110", Bytes.toString(result.getValue(family, Bytes.toBytes("addr"))));
        // 长整型类型
        assertEquals(1514334531L, Bytes.toLong(result.getValue(family, Bytes.toBytes("loginTime"))));
        assertEquals(170753157L, Bytes.toLong(result.getValue(family, Bytes.toBytes("userId"))));

        table.close();
    }

}
