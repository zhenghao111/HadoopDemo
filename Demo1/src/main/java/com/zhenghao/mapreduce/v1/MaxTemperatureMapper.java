package com.zhenghao.mapreduce.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// v1
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemprtature = Integer.parseInt(line.substring(87, 92));
        context.write(new Text(year), new IntWritable(airTemprtature));
    }
}
