package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * 参考：《Hadoop权威指南》 第四版 231页
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-13 11:59
 */
public class JobBuilder {

    public static Job parseInputAndOutput(Tool tool, Configuration conf, String [] args) throws IOException {

        System.setProperty("HADOOP_USER_NAME", "zlikun");

        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public static void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s\n\n",
                tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }

}
