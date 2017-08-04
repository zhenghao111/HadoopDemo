package com.zhenghao.io.SequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SequenceFileReadDemo {

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
