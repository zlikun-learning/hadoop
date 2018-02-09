package com.zlikun.learning.dau;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-09 09:32
 */
public class DauMapReduceLocal extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJar(new File("09-hbase-mapreduce\\target\\mr.jar").getAbsolutePath());

        // 配置输入、输出
        job.addFileToClassPath(new Path("/lib/mysql/mysql-connector-java-5.1.45.jar"));

        // 配置HBase
        Configuration conf = job.getConfiguration();
        // 设置master连接地址
        conf.set("hbase.master","m4:16010");
        // 设置连接参数：HBase数据库所在的主机IP
        conf.set("hbase.zookeeper.quorum", "m4");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // HBase 扫描器，这里取[2017/09/01, 2018/02/01)区间内登录次数
        Scan scan = new Scan();
//        // 设置扫描开始RowKey，这里取：2017/09/01 00:00:00 -> 1504195200 (秒级时间戳)
//        scan.setStartRow(Bytes.toBytes(String.format("%012d000000000000", 1504195200L)));
//        // 设置扫描结束RowKey，这里取：2018/02/01 00:00:00 -> 1517414400 (秒级时间戳)
//        scan.setStopRow(Bytes.toBytes(String.format("%012d000000000000", 1517414400L)));
//        scan.setCacheBlocks(false);

        // 初始化Mapper
        TableMapReduceUtil.initTableMapperJob("user_login_logs", scan, DauMapper.class, DateRecord.class, LongWritable.class, job);
        // 初化Reducer
        job.setReducerClass(DauReducer.class);

        // 配置MySQL
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.9.223:3306/test",
                "root",
                "ablejava");
        // 要写入的字段
        String[] fields = {"DATE", "TYPE", "USERS", "COUNT"};
        DBOutputFormat.setOutput(job, "TBL_DAU", fields);

        return job.waitForCompletion(true) ? 1 : 0 ;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new DauMapReduceLocal(), args);
        System.exit(status);
    }

}
