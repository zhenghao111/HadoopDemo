package com.zhenghao.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 * Created by zhenghao on 2017/7/7.
 */
public class ListStatus {



    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] statuses = fs.listStatus(new Path("/"));
        Path[] paths = FileUtil.stat2Paths(statuses);

        for (Path path : paths) {
            System.out.println(path);
        }
    }


}
