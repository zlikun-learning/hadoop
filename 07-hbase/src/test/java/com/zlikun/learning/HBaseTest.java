package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/14 14:37
 */
public class HBaseTest {

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

        this.tableName = TableName.valueOf("user");
    }

    @After
    public void destroy() throws IOException {
        // 关闭连接
        connection.close();
        admin.close();
    }

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void create() throws IOException {

        // 如果表存在，禁用并删除
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        // 创建表
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(new HColumnDescriptor("info"));
        descriptor.addFamily(new HColumnDescriptor("jobs"));

        // 限制region大小，超过设定值，将发生拆分操作，默认：256MB，单位：字节
//        descriptor.setMaxFileSize(0L);

        // 设置表为只读，默认：可读写
//        descriptor.setReadOnly(true);

        // 设置触发刷新缓冲区事件阈值，默认：64MB，单位：字节
//        descriptor.setMemStoreFlushSize(0L);

        // 延时日志刷写

        // 其它选项

        admin.createTable(descriptor);

    }

    /**
     * 写入数据
     * hbase(main):060:0> scan 'user'
     * ROW                   COLUMN+CELL
     *  u0001                column=info:age, timestamp=1515913607107, value=120
     *  u0001                column=info:name, timestamp=1515913607107, value=zlikun
     *  u0001                column=jobs:one, timestamp=1515913607107, value=AAA
     *  u0001                column=jobs:two, timestamp=1515913607107, value=BBB
     *  1 row(s) in 0.0160 seconds
     */
    @Test
    public void put() throws IOException {
        // 获取表连接对象
        Table table = connection.getTable(tableName);
        // 创建put对象，需要指定rowkey
        Put put = new Put(Bytes.toBytes("u0001"));
        // 添加字段信息
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zlikun"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("120"));
        put.addColumn(Bytes.toBytes("jobs"), Bytes.toBytes("one"), Bytes.toBytes("AAA"));
        put.addColumn(Bytes.toBytes("jobs"), Bytes.toBytes("two"), Bytes.toBytes("BBB"));
        // 向表中写入数据
        table.put(put);
        table.close();
    }

    @Test
    public void get() throws IOException {
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes("u0001"));
        Result result = table.get(get);
        // 遍历结果
        for (Cell cell : result.rawCells()) {
            System.out.println(String.format("row = %s, family = %s, qualifier = %s, value = %s",
                    new String(CellUtil.cloneRow(cell)),
                    new String(CellUtil.cloneFamily(cell)),
                    new String(CellUtil.cloneQualifier(cell)),
                    new String(CellUtil.cloneValue(cell))
            ));
        }
        table.close();
    }

    @Test
    public void delete() throws IOException {
        Table table = connection.getTable(tableName);
        // 指定要删除的rowkey
        Delete delete = new Delete(Bytes.toBytes("u0001"));
        // 指定要删除的列族
        delete.addFamily(Bytes.toBytes("jobs"));
        // 执行删除
        table.delete(delete);
        table.close();
    }

    @Test
    public void namespace() throws IOException {
        NamespaceDescriptor descriptor = NamespaceDescriptor
                .create("zlikun")
                .build();
        // 创建命名空间
        admin.createNamespace(descriptor);

        // 查询命名空间
        for (NamespaceDescriptor desc : admin.listNamespaceDescriptors()) {
            System.out.printf("namespace = %s\n", desc.getName());
        }
    }

}
