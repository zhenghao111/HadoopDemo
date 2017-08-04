package com.zhenghao.io.SequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SequenceFileInsertSyncDemo {

    private static final String[] DATA = {
            "One, ghjklyui ghjkghjk",
            "Two, ghjktryui bnm",
            "Three, vbnm dsjfk"
    };

    public static void main(String[] args) {
        Configuration conf = new Configuration();

        SequenceFile.Writer writer = null;

        IntWritable key = new IntWritable();
        Text value = new Text();

        try {
            writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(new Path("Demo1/file/insertsync.seq")),
                    SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(value.getClass()));

            for (int i = 0; i < 100; i++) {
                key.set(i);
                value.set(DATA[i % DATA.length]);
                System.out.println("["+writer.getLength()+"]\t"+key+"\t"+value);
                writer.append(key, value);
                writer.sync();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }

        SequenceFile.Reader reader = null;

        try {
            reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(new Path("Demo1/file/insertsync.seq")));

            IntWritable key2 = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value2 = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t&s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
