package com.zhenghao.io.SequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SequenceFileSyncDemo {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem fs = null;

        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        SequenceFile.Reader reader = null;

        try {
            reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(new Path("Demo1/file/sequencefile.seq")));

            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            reader.sync(128);
            System.out.println(reader.getPosition());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
