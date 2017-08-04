package com.zhenghao.mapreduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zhenghao on 2017/4/22.
 */
public class WordCountToMaster {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //添加参数：不使用HA方式
        conf.set("fs.defaultFS", "hdfs://nn1:9000");
        conf.set("mapreduce.jobtracker.address","nn1:8088");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname", "nn1");
        conf.set("mapreduce.job.jar", "/Users/zhenghao/Documents/Workspace/IDEA/HadoopDemo/out/artifacts/HadoopDemo1/HadoopDemo1.jar");
        //添加参数：HA方式
//        conf.set("fs.defaultFS", "hdfs://cluster1");
//        conf.set("dfs.nameservices", "cluster1");
//        conf.set("dfs.ha.namenodes.cluster1", "nn1, nn2");
//        conf.set("dfs.namenode.rpc-address.cluster1.nn1", "nn1:8020");
//        conf.set("dfs.namenode.rpc-address.cluster1.nn2", "nn2:8020");
//        conf.set("dfs.client.failover.proxy.provider.cluster1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//
//        //  conf.set("mapreduce.app-submission.cross-platform", "true");
//        conf.set("mapreduce.jobtracker.address","nn1:8088");
//        conf.set("hadoop.job.user","zhenghao");
//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.hostname", "nn1");
//        conf.set("yarn.resourcemanager.address", "nn1:8032");
//        conf.set("yarn.resourcemanager.admin.address", "nn1:8033");
//        conf.set("yarn.resourcemanager.resource-tracker.address", "nn1:8031");
//        conf.set("yarn.resourcemanager.scheduler.address", "nn1:8030");
//        conf.set("mapreduce.job.jar", "/Users/zhenghao/Documents/Workspace/IDEA/HadoopDemo/out/artifacts/HadoopDemo1/HadoopDemo1.jar");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountToMaster.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println(otherArgs[otherArgs.length - 1]);
    }
}
