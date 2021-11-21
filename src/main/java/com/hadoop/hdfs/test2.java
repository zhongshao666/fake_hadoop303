package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
获取configuration配置信息
 */
public class test2 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        String inPath = conf.get("in");
        //获取命令行中传入的参数
        FSDataInputStream in = fs.open(new Path(inPath));

        IOUtils.copyBytes(in, System.out, 1000, true);

        return 0;
    }

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new test2(), args);
    }
}
