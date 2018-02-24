package com.zlikun.learning.user.role;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-24 13:21
 */
public class RoleReducer extends Reducer<RoleRecord, LongWritable, RoleRecord, NullWritable> {

    private long yesterday ;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // 获取昨天日期时间戳，JDK8以前的日期API真TM难用
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) - 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        yesterday = calendar.getTime().getTime();
    }

    @Override
    protected void reduce(RoleRecord key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 计算用户数(去重)
        int users = 0;
        for (LongWritable val : values) {
            users ++;
        }
        key.users = users;
        key.date = yesterday;
        context.write(key, NullWritable.get());
    }

}
