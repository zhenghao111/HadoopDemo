package com.zhenghao.mapreduce.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class MaxTemperatureReducerTest {

    @Test
    public void returnsMax() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("2017"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("2017"), new IntWritable(10))
                .runTest();
    }
}
