package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * HBase下表与表间数据迁移(复制)MapReduce实现
 *
 HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath` \
 $HADOOP_HOME/bin/hadoop jar \
 07-hbase-1.0.0.jar com.zlikun.learning.Table2TableMapReducer
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/14 19:27
 */
public class Table2TableMapReducer extends Configured implements Tool {

    public static class MyTableMapper extends TableMapper<Text, Put> {
        private Text rowkey = new Text();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte [] bytes = key.get();
            rowkey.set(Bytes.toString(bytes));
            Put put = new Put(bytes);
            for (Cell cell : value.rawCells()) {
                // 只同步info列族数据
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    put.add(cell);
                }
            }
            if (!put.isEmpty()) {
                context.write(rowkey, put);
            }
        }
    }

    public static class MyTableReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put put : values) {
                context.write(null, put);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        // create job
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        // set run class
        job.setJarByClass(this.getClass());
        // set mapper
        Scan scan = new Scan();
        scan.setCaching(512);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(
                "t1",
                scan,
                MyTableMapper.class,
                Text.class,
                Put.class,
                job);
        // set reducer
        TableMapReduceUtil.initTableReducerJob(
                "t2",
                MyTableReducer.class,
                job);
        job.setNumReduceTasks(1);
        if (!job.waitForCompletion(true)) {
            System.err.println("error with job !");
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int status = ToolRunner.run(configuration, new Table2TableMapReducer(), args);
        System.exit(status);
    }

}
