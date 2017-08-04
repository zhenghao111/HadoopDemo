package com.zhenghao.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class SequenceFileWriteDemo {

    private static final String[] DATA = {
            "One, ghjklyui ghjkghjk",
            "Two, ghjktryui bnm",
            "Three, vbnm dsjfk"
    };

    public static void main(String[] args) {
        Configuration config = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(config);
        } catch (IOException e) {
            e.printStackTrace();
        }

/*
                Writer.file(),
                Writer.filesystem(fs),
                Writer.keyClass(keyClass),
                Writer.valueClass(valClass),
                Writer.bufferSize(bufferSize),
                Writer.replication(replication),
                Writer.blockSize(blockSize),
                Writer.compression(compressionType, codec),
                Writer.progressable(progress),
                Writer.metadata(metadata));
*/

        IntWritable key = new IntWritable();
        Text value = new Text();

        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(config,
                    SequenceFile.Writer.file(new Path("Demo1/file/sequencefile.seq")),
                    SequenceFile.Writer.keyClass(key.getClass()),
                    SequenceFile.Writer.valueClass(value.getClass()));

            for (int i = 0; i < 100; i++) {
                key.set(i);
                value.set(DATA[i % DATA.length]);
                System.out.println("["+writer.getLength()+"]\t"+key+"\t"+value);
                writer.append(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
