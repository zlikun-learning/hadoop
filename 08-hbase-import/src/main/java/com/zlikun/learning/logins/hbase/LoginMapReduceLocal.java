package com.zlikun.learning.logins.hbase;

import com.zlikun.learning.logins.TblRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * 使用HFile方式导入MySQL数据到HBase中
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 11:31
 */
public class LoginMapReduceLocal extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJar(new File("08-hbase-import\\target\\mr.jar").getAbsolutePath());
        // 配置MapReduce程序
        job.setMapperClass(LoginMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setReducerClass(LoginReducer.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        // 配置输入
        job.addFileToClassPath(new Path("/lib/mysql/mysql-connector-java-5.1.45.jar"));
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.9.223:3306/test",
                "root",
                "ablejava");
        // 截止 NEW_LOGIN_LOG_2017_06_18 表查询字段列表
        String[] fields = {"USER_ID", "ACCOUNT_TYPE", "OPEN_TYPE", "CLIENT_CODE", "APP_VERSION", "ADDR", "LOGIN_TIME"};
        // 从 NEW_LOGIN_LOG_2017_06_25 开始，截止NEW_LOGIN_LOG_2017_12_17，增加：DEVICE_NUMBER、IMEI两个字段
        // String[] fields = {"USER_ID", "ACCOUNT_TYPE", "OPEN_TYPE", "CLIENT_CODE", "APP_VERSION", "ADDR", "LOGIN_TIME", "DEVICE_NUMBER", "IMEI"};
        // DBInputFormat.setInput() 隐含了：job.setInputFormatClass(DBInputFormat.class);
        DBInputFormat.setInput(job, TblRecord.class,
                "NEW_LOGIN_LOG", null, null, fields);

        // 配置输出
        Configuration configuration = HBaseConfiguration.create();
        // 设置master连接地址
        configuration.set("hbase.master","m4:16010");
        // 设置连接参数：HBase数据库所在的主机IP
        configuration.set("hbase.zookeeper.quorum", "m4");
        // 设置连接参数：HBase数据库使用的端口
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("user_logins"));
        HFileOutputFormat2.configureIncrementalLoadMap(job, table);

        FileOutputFormat.setOutputPath(job, new Path("/output/11"));

        return job.waitForCompletion(true) ? 1 : 0 ;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new LoginMapReduceLocal(), args);
        System.exit(status);
    }

}
