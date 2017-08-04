package com.zhenghao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by zhenghao on 2017/7/7.
 */
public class ShowFileStatusTest {

    private MiniDFSCluster cluster;
    private DistributedFileSystem fs;

    @Before
    public void setUp() throws IOException {
        System.out.println("setUp begin...");

        Configuration conf = new Configuration();
        if (System.getProperty("test.build.data") == null) {
            System.setProperty("test.build.data", "/tmp");
        }
        cluster = new MiniDFSCluster.Builder(conf).build();
        fs = cluster.getFileSystem();
        FSDataOutputStream out = fs.create(new Path("/dir/file"));
        out.write("content".getBytes("UTF-8"));
        out.close();

        System.out.println("setUp end...");
    }

    @After
    public void tearDown() throws IOException {
        System.out.println("tearDown begin...");

        if (fs != null) {
            fs.close();
        }

        if (cluster != null) {
//            cluster.close();
        }

        System.out.println("tearDown end...");
    }

    @Test(expected = FileNotFoundException.class)
    public void throwsFileNotFoundForNonExistentFile() throws IOException {
        fs.getFileStatus(new Path("no-such-file"));
    }

    @Test
    public void fileStatusForFile() throws IOException {
        Path file = new Path("/dir/file");
        FileStatus status = fs.getFileStatus(file);
        Assert.assertThat(status.getPath().toUri().getPath(), is("/dir/file"));
        Assert.assertThat(status.isDirectory(), is(false));
        Assert.assertThat(status.getLen(), is(7L));
//        Assert.assertThat(status.getModificationTime(), is());
        Assert.assertThat(status.getReplication(), is((short)1));
        Assert.assertThat(status.getBlockSize(), is(128*1024*1024L));
        Assert.assertThat(status.getOwner(), is(System.getProperty("user.name")));
        Assert.assertThat(status.getGroup(), is("supergroup"));
        assertThat(status.getPermission().toString(), is("rw-r--r--"));
    }

    @Test
    public void fileStatusForDirectory() throws Exception {
        Path dir = new Path("/dir");
        FileStatus status = fs.getFileStatus(dir);
        assertThat(status.getPath().toUri().getPath(), is("/dir"));
        assertThat(status.isDirectory(), is(true));
        assertThat(status.getLen(), is(0L));
        assertThat(status.getReplication(), is((short)0));
        assertThat(status.getBlockSize(), is(0L));
        assertThat(status.getOwner(), is(System.getProperty("user.name")));
        assertThat(status.getGroup(), is("supergroup"));
        assertThat(status.getPermission().toString(), is("rwxr-xr-x"));
    }
}
