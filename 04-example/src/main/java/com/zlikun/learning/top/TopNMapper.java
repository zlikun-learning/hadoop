package com.zlikun.learning.top;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 使用自定义键进行映射(主要使用其比较方法，以实现排序)
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 11:07
 */
public class TopNMapper extends Mapper<LongWritable, Text, NewKey, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        String key0 = null;
        if (tokenizer.hasMoreTokens()) {
            key0 = tokenizer.nextToken().trim();
        }
        int value0 = -1;
        if (tokenizer.hasMoreTokens()) {
            value0 = NumberUtils.toInt(tokenizer.nextToken(), -1);
        }
        if (key0 != null && value0 >= 0) {
            context.write(new NewKey(key0, value0), NullWritable.get());
        }
    }
}
