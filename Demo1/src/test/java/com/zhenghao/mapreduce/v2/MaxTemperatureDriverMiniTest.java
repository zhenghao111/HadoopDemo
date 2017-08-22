package com.zhenghao.mapreduce.v2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;

public class MaxTemperatureDriverMiniTest extends ClusterMapReduceTestCase {



    public static class OutputLogFilter implements PathFilter {

        @Override
        public boolean accept(Path path) {
            return !path.getName().startsWith("_");
        }
    }

    @Override
    protected void setUp() throws Exception {
        if (System.getProperty("test.build.data") == null) {
            System.setProperty("test.build.data", "data/tmp");
        }






        super.setUp();
    }

    public void testName() throws Exception {
    }
}
