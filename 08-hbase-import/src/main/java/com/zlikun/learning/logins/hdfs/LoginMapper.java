package com.zlikun.learning.logins.hdfs;

import com.zlikun.learning.logins.TblRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 11:35
 */
public class LoginMapper extends Mapper<LongWritable, TblRecord, Text, NullWritable> {

    /**
     * 遍历MySQL表数据，key为表主键，值为自定义对象
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, TblRecord value, Mapper.Context context) throws IOException, InterruptedException {

        // 无效数据过滤
        if (value.userId == null || value.loginTime == null) {
            return;
        }

        context.write(new Text(value.toString()), NullWritable.get());

    }

}
