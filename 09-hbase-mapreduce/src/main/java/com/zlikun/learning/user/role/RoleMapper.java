package com.zlikun.learning.user.role;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-24 13:20
 */
public class RoleMapper extends TableMapper<RoleRecord, LongWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        int role = Bytes.toInt(value.getValue(Bytes.toBytes("i"), Bytes.toBytes("role")));
        long userId = Bytes.toInt(value.getValue(Bytes.toBytes("i"), Bytes.toBytes("userId")));
        context.write(new RoleRecord(role), new LongWritable(userId));
    }

}
