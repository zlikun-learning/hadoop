package com.zlikun.learning.cache;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

/**
 * 测试分布式缓存机制
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-23 14:19
 */
public class DistributedCacheMapReduce extends Configured implements Tool {

    public static class CacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        /**
         * 使用分布式缓存机制，加载学校 -> 省份对应关系数据
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // I、直接读取HDFS文件数据
            hdfs(context);

            // II、使用符号链接方式访问(#symlink)
//            symlink(context);

        }

        void hdfs(Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration()) ;
            FileStatus[] statuses = fs.listStatus(new Path("/basic/province"));
            for (FileStatus status : statuses) {
                if (!status.isFile()) continue;
                InputStream in = null ;
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte [] buf = null;
                try {
                    in = fs.open(status.getPath()) ;
//                    IOUtils.copyBytes(in, System.out, 4096, false);
                    IOUtils.copyBytes(in, out, 4096, false);
                    buf = out.toByteArray();
                } finally {
                    IOUtils.closeStream(out);
                    IOUtils.closeStream(in);
                }
                if (buf != null) {
                    System.out.println(new String(buf, "UTF-8"));
                }
            }
        }

        void symlink(Context context) throws IOException {
            URI [] uris = context.getCacheFiles();
            if (uris != null && uris.length > 0) {
                for (URI uri : uris) {
                    File file = new File(uri.getFragment());
                    List<String> lines = FileUtils.readLines(file);
                    for (String line : lines) {
                        System.out.println(line);
                    }
                }
            }
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJar(new File("02-mapreduce\\target\\mr.jar").getAbsolutePath());
        // 这里必须是文件，且必须指定完整HDFS文件路径
        job.addCacheFile(new URI("hdfs://m4:9000/basic/province/part-m-00000#symlink"));

        job.setMapperClass(CacheMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path("/input"));
        FileOutputFormat.setOutputPath(job, new Path("/output/35"));

        return job.waitForCompletion(true) ? 1 : 0 ;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new DistributedCacheMapReduce(), args);
        System.exit(status);
    }

}
