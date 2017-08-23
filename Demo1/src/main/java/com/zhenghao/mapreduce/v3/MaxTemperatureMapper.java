package com.zhenghao.mapreduce.v3;

import com.zhenghao.mapreduce.v2.NcdcRecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    //自定义计数器
    enum Temperature {
        OVER_100
    }

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        parser.parser(value);
        if (parser.isValidTemperature()) {
            int airTempature = parser.getAirTemperature();
            if (airTempature > 100) {
                System.err.println("温度超过100度：" + value);
                context.setStatus("发现错误记录：查logs.");
                context.getCounter(Temperature.OVER_100).increment(1);
            }

            context.write(new Text(parser.getYear()), new IntWritable(airTempature));
        }
    }

}
