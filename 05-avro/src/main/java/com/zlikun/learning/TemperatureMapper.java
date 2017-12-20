package com.zlikun.learning;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 映射年份和气温
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-20 09:24
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    private GenericRecord record = new GenericData.Record(MaxTemperature.SCHEMA);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        parser.parse(value);

        if (parser.isValidTemperature()) {
            record.put("stationId", parser.getStationId());
            record.put("year", parser.getYearInt());
            record.put("temperature", parser.getAirTemperature());
            context.write(new AvroKey<>(parser.getYearInt()), new AvroValue<>(record));
        }

    }
}
