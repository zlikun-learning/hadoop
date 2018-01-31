package com.zlikun.learning.logins.mongo;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

/**
 * 读取Mongo数据，按HBase数据格式输出
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-31 11:11
 */
public class LoginMapper extends Mapper<Object, BSONObject, ImmutableBytesWritable, KeyValue> {

    @Override
    protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        System.out.println(value.keySet());
    }

}
