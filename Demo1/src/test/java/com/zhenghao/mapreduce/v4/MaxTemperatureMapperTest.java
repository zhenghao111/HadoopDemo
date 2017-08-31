package com.zhenghao.mapreduce.v4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import java.io.IOException;


public class MaxTemperatureMapperTest {

    @Test
    public void parsersMissingTemperature() throws IOException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382 9999V0203201N00261220001CN9999999N9+99991+99999999999");
        //温度为999无效，没有输出
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .runTest();
    }

    @Test
    public void parsresMalformedTemperature() throws IOException {
        Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004RJSN V02011359003150070356999999433201957010100005+353");

        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withCounters(counters)
                .runTest();

        Counter counter = counters.findCounter(MaxTemperatureMapper.Temperature.MALFORMED);
        assertThat(counter.getValue(), is(1L));
    }
}
