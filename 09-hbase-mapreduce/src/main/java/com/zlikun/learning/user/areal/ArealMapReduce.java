package com.zlikun.learning.user.areal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Calendar;

/**
 * 学生、教师注册人数按省份分布统计
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-09 09:32
 */
public class ArealMapReduce extends Configured implements Tool {

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

        // HBase 扫描器，这里取[, 今天0点)区间内认证用户数据
        Scan scan = new Scan();
        // 设置扫描结束RowKey，取今天0点时间戳(实际不包含)
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        scan.setStopRow(Bytes.toBytes(String.format("%012d000000000000", calendar.getTimeInMillis() / 1000)));

        // 初始化Mapper
        TableMapReduceUtil.initTableMapperJob("certify", scan, ArealMapper.class, ArealRecord.class, LongWritable.class, job);
        // 初化Reducer
        job.setReducerClass(ArealReducer.class);

        // 配置MySQL
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.9.223:3306/test",
                "root",
                "ablejava");
        // 要写入的字段
        String[] fields = {"PROVINCE", "ROLE", "USERS"};
        DBOutputFormat.setOutput(job, "TBL_USER_AREAL", fields);

        return job.waitForCompletion(true) ? 1 : 0 ;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new ArealMapReduce(), args);
        System.exit(status);
    }

}
