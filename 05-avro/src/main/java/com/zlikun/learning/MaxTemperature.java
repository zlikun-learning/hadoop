package com.zlikun.learning;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 使用Avro实现计算每年最高气温值任务
 *
 * http://avro.apache.org/docs/current/mr.html
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-20 14:59
 */
public class MaxTemperature {

    public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"WeatherRecord\",\"namespace\":\"com.zlikun.learning.avro\",\"doc\":\"A weather reading.\",\"fields\":[{\"name\":\"year\",\"type\":\"int\"},{\"name\":\"temperature\",\"type\":\"int\"},{\"name\":\"stationId\",\"type\":\"string\"}]}");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length == 0) {
            args = new String[]{
                    "hdfs://zlikun:9000/ncdc",
                    "hdfs://zlikun:9000/output/46"
            };
        }

        // 设置用户名
        String os = System.getenv("os");
        if (os != null && os.startsWith("Windows")) {
            System.setProperty("HADOOP_USER_NAME", "zlikun");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max_temperature_by_avro");
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, SCHEMA);
        AvroJob.setOutputKeySchema(job, SCHEMA);
        // AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.NULL));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        if (os != null && os.startsWith("Windows")) {
            job.setJar(new File("05-avro\\target\\mr.jar").getAbsolutePath());
        } else {
            job.setJarByClass(MaxTemperature.class);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true) ;
        if (status) {
            System.out.println("word_count job is complete .");
        }
        System.exit(status ? 0 : 1);

    }

}
