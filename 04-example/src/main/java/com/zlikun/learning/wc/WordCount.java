package com.zlikun.learning.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 词频统计MR程序
 * http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 10:19
 */
public class WordCount {

    /**
     * 使用hadoop执行mr程序
     *      hadoop jar jars/mr.jar com.zlikun.learning.wc.WordCount /lyric /output/22
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length == 0) {
            args = new String[]{
                    "hdfs://zlikun:9000/lyric",
                    "hdfs://zlikun:9000/output/21"
            };
        }

        // 设置用户名
        String os = System.getenv("os");
        if (os != null && os.startsWith("Windows")) {
            System.setProperty("HADOOP_USER_NAME", "zlikun");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word_count");
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        if (os != null && os.startsWith("Windows")) {
            job.setJar(new File("04-example\\target\\mr.jar").getAbsolutePath());
        } else {
            job.setJarByClass(WordCount.class);
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

/* --------------------------------
$ hdfs dfs -cat /output/20/*
And	1
Big	2
But	6
Emilia	1
I	12
I'm	6
In	5
It's	7
Like	1
Miss	5
Outside	1
Why	2
World	1
You're	1
a	15
all	2
and	1
are	1
arms	1
around	1
big	30
can	1
cold	1
did	2
do	9
end	1
eyes	2
falling	2
feel	5
feeling	1
fire	1
first	1
from	1
girl	5
gone.	1
happen	1
have	3
if	5
inside	1
it	2
it's	1
leaf	1
leave	5
like	1
me	6
miss	5
much	5
much.	5
my	2
nice	1
not	5
now	1
ooooh	1
open	1
outside	1
raining	1
see	1
so	1
tears	1
that	5
the	2
thing	5
to	2
too	8
very	1
way	1
when	1
will	5
world	5
yellow	1
you	15
your	1
-------------------------------- */
