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
 * PUT 命令测试
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/2/1 21:00
 */
public class PutTest {

    private Configuration configuration;
    private Connection connection;
    private Table table;

    @Before
    public void init() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
        this.table = connection.getTable(TableName.valueOf("user"));
    }

    @After
    public void destroy() throws IOException {
        connection.close();
        table.close();
    }

    @Test
    public void put() throws IOException {

        byte[] family = Bytes.toBytes("info");

        // 使用列族和时间戳构造Put实例
        Put put = new Put(Bytes.toBytes("0001"), System.currentTimeMillis());
        // 添加两列(Cell)，即：插入两条记录
        put.addColumn(family, Bytes.toBytes("name"), Bytes.toBytes("zlikun"));
        put.addColumn(family, Bytes.toBytes("age"), Bytes.toBytes(17));

        // 执行插入
        table.put(put);

        // 验证写入
        Result result = table.get(new Get(Bytes.toBytes("0001")));
        assertEquals("zlikun", Bytes.toString(result.getValue(family, Bytes.toBytes("name"))));
        assertEquals(17, Bytes.toInt(result.getValue(family, Bytes.toBytes("age"))));

    }

}
