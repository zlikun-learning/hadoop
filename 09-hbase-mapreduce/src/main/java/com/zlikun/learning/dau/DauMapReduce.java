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
public class DauMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        // 配置HBase
        Configuration conf = job.getConfiguration();
        // 设置master连接地址
        conf.set("hbase.master",args[4]);

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
                args[0],
                args[1],
                args[2]);
        // 要写入的字段
        String[] fields = {"DATE", "TYPE", "USERS", "COUNT"};
        DBOutputFormat.setOutput(job, args[3], fields);

        return job.waitForCompletion(true) ? 1 : 0 ;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: com.zlikun.learning.dau.DauMapReduce <jdbcUrl> <username> <password> <tableName> <hMasterSever>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new DauMapReduce(), args);
        System.exit(status);
    }

}
