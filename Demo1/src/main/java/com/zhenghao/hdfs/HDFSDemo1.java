package com.zhenghao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by zhenghao on 2017/4/22.
 */
public class HDFSDemo1 {

    Configuration conf = null;
    FileSystem fs = null;

    public HDFSDemo1() throws IOException {
        conf = new Configuration();
        fs = FileSystem.get(conf);
//        System.out.println(conf.get("fs.defaultFS"));
//        System.out.println(fs);
    }

    /**
     * 读取HDFS上的文件
     *
     * @param fileName
     */
    private void read(String fileName) {

        FSDataInputStream inputStream = null;

        try {
            inputStream = fs.open(new Path(fileName));
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }
    }

    /**
     * 拷贝
     */
    private void copyFromLocal(String localFile, String hdfsFile) {

        InputStream in = null;
        FSDataOutputStream out = null;

        try {

            in = new BufferedInputStream(new FileInputStream(localFile));
            out = fs.create(new Path(hdfsFile), new Progressable() {
                @Override
                public void progress() {
                    System.out.print('#');
                }
            });

            IOUtils.copyBytes(in, out, 4096, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void test() throws Exception {
//        fs.copyFromLocalFile(new Path("data/inputword"), new Path("/data1/inputword"));
//        System.out.println(fs.exists(new Path("/data1/inputword")));
//        FileStatus[] fileStatus = fs.listStatus(new Path("/data1/inputword"));
//        System.out.println(fileStatus[0].getPath());
//        System.out.println(fileStatus[0].getBlockSize());
//        System.out.println(fileStatus[0].getReplication());
//        System.out.println(fs.delete(new Path("/data1/inputword"), false));

    }

    public static void main(String[] args) throws IOException {
        HDFSDemo1 demo1 = new HDFSDemo1();
        //读取HDFS上的文件
//        demo1.read("/result/part-r-00000");
        //拷贝
        demo1.copyFromLocal("/Users/zhenghao/Documents/Workspace/IDEA/HadoopDemo/HadoopDemo1/resources/core-site.xml", "/core-site.xml");

    }
}
