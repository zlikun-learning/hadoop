package com.zlikun.learning;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-20 09:26
 */
public class TemperatureReducer extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

    @Override
    protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {

        GenericRecord maxRecord = null;
        for (AvroValue<GenericRecord> val : values) {
            GenericRecord record = val.datum();
            if (maxRecord == null) {
                maxRecord = record;
                continue;
            }
            if ((Integer) maxRecord.get("temperature") < (Integer) record.get("temperature")) {
                maxRecord = record;
            }
        }
        context.write(new AvroKey<>(maxRecord), NullWritable.get());

    }

}
