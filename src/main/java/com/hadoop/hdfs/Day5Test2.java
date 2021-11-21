package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Day5Test2 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/a.txt"));

        FSDataInputStream open = fs.open(new Path("/a"));
        //编解码工厂
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

        //根据名字获取编解码器
        //CompressionCodec codec = factory.getCodec(new Path("/a.gz"));
        //使用工厂获取编解码器
        CompressionCodec gzip = factory.getCodecByName("gzip");
        //具有编解码功能包装流

        CompressionInputStream inputStream = gzip.createInputStream(open);

//        IOUtils.copyBytes(inputStream,System.out,100,true);

        int len = 0;
        while ((len=inputStream.read())!=-1){
            fsDataOutputStream.write(len);
            fsDataOutputStream.flush();
            System.out.write(len);
            System.out.flush();
        }
        fsDataOutputStream.close();
        inputStream.close();

        fs.close();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Day5Test2(), args);
    }
}