package com.zhenghao.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zhenghao on 2017/4/22.
 */
public class Template extends Configured implements Tool{

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //nothing
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //nothing
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //nothing
        }

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

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //nothing
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Template.class);

        // 1. input
        for (int i = 0; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        // 2. map
        job.setMapperClass(TokenizerMapper.class);
//        job.setMapOutputKeyClass();
//        job.setMapOutputValueClass();

        // 3. shuffle
        // 3.1 分区
//        job.setPartitionerClass();

        // 3.2 排序
//        job.setSortComparatorClass();

        // 3.3 Combiner,Map端reduce
        job.setCombinerClass(IntSumReducer.class);

        // 3.4 分组
//        job.setGroupingComparatorClass();

        // 3.5 压缩
//        conf.set("mapreduce.map.output.compres", "true");
//        conf.set("mapreduce.map.output.compress.codec", "");

        // 4. reudce
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4.1 reduce任务数
//        job.setNumReduceTasks();

        // 5. output
        FileOutputFormat.setOutputPath(job,
                new Path(args[args.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.jar", "/Users/zhenghao/Documents/Workspace/IDEA/HadoopDemo/out/artifacts/HadoopDemo1/HadoopDemo1.jar");
        //ToolRunner.run(new Template(), args);/会创建Configuration
        int status = ToolRunner.run(conf, new Template(), args);
        System.exit(status);
    }
}
